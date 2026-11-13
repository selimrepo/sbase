package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Real Kafka + PostgreSQL failure-path tests for tenant-safe CDC ingestion semantics. */
@Testcontainers(disabledWithoutDocker = true)
class KafkaDebeziumEndToEndIsolationTest {
    static final UUID A=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    static final UUID B=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    @Container static final KafkaContainer KAFKA=new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));
    @Container static final PostgreSQLContainer<?> PG=new PostgreSQLContainer<>("postgres:16-alpine");
    HikariDataSource ds; JdbcTemplate db; DurableRecordSink sink; ObjectMapper json=new ObjectMapper();

    @BeforeEach void setup() {
        ds=new HikariDataSource(); ds.setJdbcUrl(PG.getJdbcUrl()); ds.setUsername(PG.getUsername()); ds.setPassword(PG.getPassword());
        db=new JdbcTemplate(ds);
        db.execute("CREATE TABLE IF NOT EXISTS ingestion_connectors(id UUID PRIMARY KEY,tenant_id UUID NOT NULL)");
        db.execute("""
          CREATE TABLE IF NOT EXISTS ingestion_partition_offsets(tenant_id UUID NOT NULL,connector_id UUID NOT NULL,
          topic VARCHAR(249) NOT NULL,partition_id INT NOT NULL,next_offset BIGINT NOT NULL,fencing_token BIGINT NOT NULL DEFAULT 0,
          lease_owner VARCHAR(160),lease_until TIMESTAMPTZ,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(tenant_id,connector_id,topic,partition_id))""");
        db.execute("""
          CREATE TABLE IF NOT EXISTS ingestion_batch_receipts(tenant_id UUID NOT NULL,connector_id UUID NOT NULL,
          topic VARCHAR(249) NOT NULL,partition_id INT NOT NULL,first_offset BIGINT NOT NULL,next_offset BIGINT NOT NULL,
          record_count INT NOT NULL,PRIMARY KEY(tenant_id,connector_id,topic,partition_id,first_offset,next_offset))""");
        db.execute("""
          CREATE TABLE IF NOT EXISTS ingestion_records(id UUID PRIMARY KEY,connector_id UUID NOT NULL,tenant_id UUID NOT NULL,
          source_key TEXT NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL,UNIQUE(connector_id,source_key))""");
        db.execute("TRUNCATE ingestion_records,ingestion_batch_receipts,ingestion_partition_offsets,ingestion_connectors");
        sink=new DurableRecordSink(db,new TransactionTemplate(new DataSourceTransactionManager(ds)),json);
    }
    @AfterEach void close(){if(ds!=null)ds.close();}

    @Test void realGroupRebalanceFencesOldPartitionOwner() throws Exception {
        String topic=topic(); createTopic(topic,2); UUID connector=connector(A);
        String group="rebalance-"+UUID.randomUUID();
        try(KafkaConsumer<String,String> one=consumer(group); KafkaConsumer<String,String> two=consumer(group)) {
            one.subscribe(List.of(topic)); Set<TopicPartition> before=assignment(one);
            assertFalse(before.isEmpty());
            two.subscribe(List.of(topic));
            Set<TopicPartition> a=assignment(one), b=assignment(two);
            Set<TopicPartition> all=new HashSet<>(a); all.addAll(b);
            assertEquals(Set.of(new TopicPartition(topic,0),new TopicPartition(topic,1)),all);
            Set<TopicPartition> overlap=new HashSet<>(a); overlap.retainAll(b); assertTrue(overlap.isEmpty());
            TopicPartition moved=b.iterator().next();
            long old=sink.claimKafkaPartition(connector,topic,moved.partition(),"old-owner");
            sink.releaseKafkaPartition(connector,topic,moved.partition(),"old-owner",old);
            long fresh=sink.claimKafkaPartition(connector,topic,moved.partition(),"new-owner");
            assertTrue(fresh>old);
            assertThrows(IllegalStateException.class,()->sink.writeKafkaBatch(connector,topic,
              List.of(envelope(topic,moved.partition(),0,A)),"old-owner",Map.of(moved.partition(),old)));
            assertEquals(0,count(connector));
        }
    }

    @Test void crashAfterSinkCommitReplaysFromBrokerWithoutDuplicateRows() throws Exception {
        String topic=topic(); createTopic(topic,1); UUID connector=connector(A);
        send(topic,"cdc",A,"{\"tenant_id\":\""+A+"\",\"op\":\"c\"}");
        TopicPartition tp=new TopicPartition(topic,0); String group="crash-"+UUID.randomUUID();
        KafkaConsumer<String,String> dead=consumer(group); dead.assign(List.of(tp)); dead.seekToBeginning(List.of(tp));
        ConsumerRecord<String,String> first=record(dead);
        TenantEnvelopeValidator.requireTrustedHeader(first.headers(),"sbase-tenant-id",A);
        TenantEnvelopeValidator.requireValidJsonTenantIfPresent(json,first.value(),A);
        long old=sink.claimKafkaPartition(connector,topic,0,"before-crash");
        assertEquals(1,sink.writeKafkaBatch(connector,topic,List.of(envelope(topic,0,first.offset(),A)),
          "before-crash",Map.of(0,old)));
        dead.close(); // process died before Kafka offset commit
        try(KafkaConsumer<String,String> restarted=consumer(group)) {
            restarted.assign(List.of(tp)); restarted.seek(tp,first.offset());
            ConsumerRecord<String,String> replay=record(restarted);
            assertEquals(first.offset(),replay.offset());
            long token=sink.claimKafkaPartition(connector,topic,0,"after-crash");
            assertEquals(0,sink.writeKafkaBatch(connector,topic,List.of(envelope(topic,0,replay.offset(),A)),
              "after-crash",Map.of(0,token)));
            restarted.commitSync(Map.of(tp,new OffsetAndMetadata(replay.offset()+1)));
            assertEquals(1,count(connector));
        }
    }

    @Test void foreignTenantMessageOnSharedTopicNeverReachesOtherTenantSink() throws Exception {
        String topic=topic(); createTopic(topic,1); UUID ca=connector(A), cb=connector(B);
        send(topic,"for-a",A,"{\"tenant_id\":\""+A+"\"}");
        send(topic,"for-b",B,"{\"tenant_id\":\""+B+"\"}");
        TopicPartition tp=new TopicPartition(topic,0);
        try(KafkaConsumer<String,String> readerA=consumer("reader-a-"+UUID.randomUUID());
            KafkaConsumer<String,String> readerB=consumer("reader-b-"+UUID.randomUUID())) {
            readerA.assign(List.of(tp)); readerB.assign(List.of(tp));
            readerA.seekToBeginning(List.of(tp)); readerB.seekToBeginning(List.of(tp));
            List<ConsumerRecord<String,String>> recordsA=records(readerA,2), recordsB=records(readerB,2);
            ConsumerRecord<String,String> foreign=recordsA.stream().filter(r->"for-b".equals(r.key())).findFirst().orElseThrow();
            assertThrows(SecurityException.class,()->{
                TenantEnvelopeValidator.requireTrustedHeader(foreign.headers(),"sbase-tenant-id",A);
                TenantEnvelopeValidator.requireValidJsonTenantIfPresent(json,foreign.value(),A);
            });
            assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(
              json,"{\"tenant_id\":\""+B+"\"}",A));
            assertEquals(0,count(ca));
            ConsumerRecord<String,String> own=recordsB.stream().filter(r->"for-b".equals(r.key())).findFirst().orElseThrow();
            TenantEnvelopeValidator.requireTrustedHeader(own.headers(),"sbase-tenant-id",B);
            long token=sink.claimKafkaPartition(cb,topic,0,"tenant-b-owner");
            assertEquals(1,sink.writeKafkaBatch(cb,topic,List.of(envelope(topic,0,own.offset(),B)),
              "tenant-b-owner",Map.of(0,token)));
            assertEquals(0,count(ca)); assertEquals(1,count(cb));
            assertEquals(1,db.queryForObject("SELECT count(*) FROM ingestion_records WHERE tenant_id=?",Integer.class,B));
        }
    }

    UUID connector(UUID tenant){UUID id=UUID.randomUUID();db.update("INSERT INTO ingestion_connectors VALUES(?,?)",id,tenant);return id;}
    long count(UUID id){return db.queryForObject("SELECT count(*) FROM ingestion_records WHERE connector_id=?",Long.class,id);}
    DurableRecordSink.Envelope envelope(String topic,int partition,long offset,UUID tenant){
        return new DurableRecordSink.Envelope(topic+":"+partition+":"+offset,
          Map.of("tenant_id",tenant.toString(),"topic",topic,"partition",partition,"offset",offset,"value","cdc"));
    }
    String topic(){return "cdc-"+UUID.randomUUID();}
    void createTopic(String topic,int count)throws Exception{
        try(Admin admin=Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers()))){
            admin.createTopics(List.of(new NewTopic(topic,count,(short)1))).all().get(10,TimeUnit.SECONDS);
        }
    }
    void send(String topic,String key,UUID tenant,String value)throws Exception{
        Properties p=new Properties();p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        try(KafkaProducer<String,String> producer=new KafkaProducer<>(p)){
            RecordHeaders h=new RecordHeaders();h.add("sbase-tenant-id",tenant.toString().getBytes(StandardCharsets.UTF_8));
            producer.send(new ProducerRecord<>(topic,0,null,key,value,h)).get(10,TimeUnit.SECONDS);
        }
    }
    KafkaConsumer<String,String> consumer(String group){
        Properties p=new Properties();p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG,group);p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"6000");p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"2000");
        return new KafkaConsumer<>(p);
    }
    Set<TopicPartition> assignment(KafkaConsumer<String,String> c){
        long end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
        while(System.nanoTime()<end){c.poll(Duration.ofMillis(200));if(!c.assignment().isEmpty())return new HashSet<>(c.assignment());}
        fail("No Kafka partition assignment");return Set.of();
    }
    ConsumerRecord<String,String> record(KafkaConsumer<String,String> c){return records(c,1).get(0);}
    List<ConsumerRecord<String,String>> records(KafkaConsumer<String,String> c,int expected){
        List<ConsumerRecord<String,String>> result=new ArrayList<>();long end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
        while(result.size()<expected&&System.nanoTime()<end)for(ConsumerRecord<String,String> r:c.poll(Duration.ofMillis(250)))result.add(r);
        assertTrue(result.size()>=expected,"Timed out waiting for broker records");return result;
    }
}