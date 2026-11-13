package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;

/** Consumes Debezium Kafka Connect topics and persists the complete change envelope before committing offsets. */
@Component
public class DebeziumIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink;
    private final ObjectMapper mapper;
    public DebeziumIngestionConnector(DurableRecordSink sink,ObjectMapper mapper) { this.sink=sink;this.mapper=mapper; }
    @Override public String type() { return "debezium"; }

    @Override public ConnectorResult ingest(ConnectorRequest request) {
        Map<String,String> o=request.options();
        UUID id=UUID.fromString(required(o,"connectorId"));
        UUID tenantId=UUID.fromString(required(o,"tenantId"));
        String tenantHeader=o.getOrDefault("tenantHeader","sbase-tenant-id");
        String topic=required(o,"topic");
        Properties p=new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,required(o,"bootstrapServers"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG,TenantEnvelopeValidator.tenantScopedConsumerGroup(
            o.getOrDefault("groupId","sbase-cdc"),tenantId,id));
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,o.getOrDefault("autoOffsetReset","earliest"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        int max=Integer.parseInt(o.getOrDefault("maxRecords","500"));
        long read=0,written=0,deadline=System.currentTimeMillis()+Long.parseLong(o.getOrDefault("pollTimeoutMs","5000"));
        try(KafkaConsumer<String,String> consumer=new KafkaConsumer<>(p)){
            consumer.subscribe(List.of(topic));
            while(read<max && System.currentTimeMillis()<deadline){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(500));
                if(records.isEmpty())continue;
                List<DurableRecordSink.Envelope> batch=new ArrayList<>();
                Map<TopicPartition,Long> offsets=new HashMap<>();
                for(ConsumerRecord<String,String> r:records){
                    TenantEnvelopeValidator.requireTrustedHeader(r.headers(),tenantHeader,tenantId);
                    TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,r.value(),tenantId);
                    String sourceKey=topic+":"+r.partition()+":"+r.offset();
                    Map<String,Object> payload=new LinkedHashMap<>();
                    payload.put("tenant_id",tenantId.toString());
                    payload.put("key",r.key()); payload.put("envelope",r.value());
                    payload.put("topic",topic); payload.put("partition",r.partition()); payload.put("offset",r.offset());
                    // Debezium's before/after/op/source/ts_ms envelope is retained without lossy flattening.
                    batch.add(new DurableRecordSink.Envelope(sourceKey,payload));
                    offsets.put(new TopicPartition(topic,r.partition()),r.offset()+1);
                }
                written+=sink.write(id,topic,batch,"debezium:"+topic,encode(offsets));
                Map<TopicPartition,OffsetAndMetadata> commit=new HashMap<>();
                offsets.forEach((tp,off)->commit.put(tp,new OffsetAndMetadata(off)));
                consumer.commitSync(commit);
                read+=records.count();
            }
        }catch(Exception e){throw new IllegalStateException("Debezium CDC persistence failed before offset acknowledgement",e);}
        return new ConnectorResult(read,written,"Debezium change envelopes (before/after/op/source) persisted; offsets committed after sink");
    }
    private static String encode(Map<TopicPartition,Long> m){StringJoiner j=new StringJoiner(",");m.forEach((k,v)->j.add(k.partition()+":"+v));return j.toString();}
    private static String required(Map<String,String> o,String k){String v=o.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
}
