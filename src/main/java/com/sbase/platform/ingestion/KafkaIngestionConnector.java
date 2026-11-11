package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Component
public class KafkaIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink;
    private final ObjectMapper mapper;
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    public KafkaIngestionConnector(DurableRecordSink sink,ObjectMapper mapper) { this.sink=sink; this.mapper=mapper; }
    @Override public String type() { return "kafka"; }

    @Override public ConnectorResult ingest(ConnectorRequest request) {
        Map<String,String> o=request.options();
        UUID connectorId=UUID.fromString(required(o,"connectorId"));
        String topic=required(o,"topic"), bootstrap=required(o,"bootstrapServers");
        String group=o.getOrDefault("groupId","sbase-"+connectorId);
        String registry=o.getOrDefault("schemaRegistryUrl","");
        int max=Integer.parseInt(o.getOrDefault("maxRecords","500"));
        if(max<1 || max>10000) throw new IllegalArgumentException("maxRecords must be between 1 and 10000");
        Properties p=new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,o.getOrDefault("autoOffsetReset","earliest"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,max);
        long read=0,written=0;
        String ownerId=UUID.randomUUID().toString();
        Map<Integer,Long> partitionTokens=new java.util.concurrent.ConcurrentHashMap<>();
        try(KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    for (TopicPartition tp : partitions) {
                        Long token=partitionTokens.remove(tp.partition());
                        if (token!=null) sink.releaseKafkaPartition(connectorId,topic,tp.partition(),ownerId,token);
                    }
                }
                @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition tp : partitions) {
                        long token=sink.claimKafkaPartition(connectorId,topic,tp.partition(),ownerId);
                        partitionTokens.put(tp.partition(),token);
                    }
                }
                @Override public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    onPartitionsRevoked(partitions);
                }
            });
            long deadline=System.currentTimeMillis()+Long.parseLong(o.getOrDefault("pollTimeoutMs","5000"));
            long lastLeaseRenewal=0;
            while(read<max && System.currentTimeMillis()<deadline) {
                ConsumerRecords<String,byte[]> batch=consumer.poll(Duration.ofMillis(500));
                long now=System.currentTimeMillis();
                if(now-lastLeaseRenewal>=30000) {
                    for(var lease : partitionTokens.entrySet()) {
                        if(!sink.renewKafkaPartition(connectorId,topic,lease.getKey(),ownerId,lease.getValue()))
                            throw new IllegalStateException("Kafka partition lease expired or was fenced: "+lease.getKey());
                    }
                    lastLeaseRenewal=now;
                }
                if(batch.isEmpty()) continue;
                List<DurableRecordSink.Envelope> envelopes=new ArrayList<>();
                Map<TopicPartition,Long> nextOffsets=new HashMap<>();
                for(ConsumerRecord<String,byte[]> r:batch) {
                    String key=topic+":"+r.partition()+":"+r.offset();
                    byte[] bytes=r.value()==null?new byte[0]:r.value();
                    Map<String,Object> payload=new LinkedHashMap<>();
                    payload.put("topic",topic); payload.put("partition",r.partition()); payload.put("offset",r.offset());
                    payload.put("key",r.key()); payload.put("timestamp",r.timestamp());
                    payload.put("valueBase64",Base64.getEncoder().encodeToString(bytes));
                    if(bytes.length>=5 && bytes[0]==0) {
                        int schemaId=ByteBuffer.wrap(bytes,1,4).getInt();
                        payload.put("schemaId",schemaId);
                        payload.put("confluentWireFormat",true);
                        if(!registry.isBlank()) payload.put("schema",resolveSchema(registry,schemaId));
                        payload.put("valueBase64",Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes,5,bytes.length)));
                    } else {
                        payload.put("confluentWireFormat",false);
                        payload.put("value",new String(bytes,StandardCharsets.UTF_8));
                    }
                    envelopes.add(new DurableRecordSink.Envelope(key,payload));
                    nextOffsets.put(new TopicPartition(topic,r.partition()),r.offset()+1);
                }
                Map<Integer,Long> batchTokens=new HashMap<>();
                for (TopicPartition tp : nextOffsets.keySet()) {
                    Long token=partitionTokens.get(tp.partition());
                    if (token==null) throw new IllegalStateException("Kafka partition is no longer owned by this consumer: "+tp);
                    batchTokens.put(tp.partition(),token);
                }
                written+=sink.writeKafkaBatch(connectorId,topic,envelopes,ownerId,batchTokens);
                Map<TopicPartition,OffsetAndMetadata> commits=new HashMap<>();
                nextOffsets.forEach((tp,off)->commits.put(tp,new OffsetAndMetadata(off)));
                consumer.commitSync(commits);
                read+=batch.count();
            }
        } catch(Exception e) {
            throw new IllegalStateException("Kafka durable ingestion failed; offsets were not acknowledged before sink persistence",e);
        }
        return new ConnectorResult(read,written,"Persisted to PostgreSQL; Confluent schema IDs and raw payload bytes are retained; offsets committed after durable sink transaction");
    }

    private JsonNode resolveSchema(String registry,int schemaId) throws Exception {
        String base=registry.replaceAll("/+$","");
        HttpRequest request=HttpRequest.newBuilder(URI.create(base+"/schemas/ids/"+schemaId))
            .timeout(Duration.ofSeconds(5)).header("Accept","application/vnd.schemaregistry.v1+json")
            .GET().build();
        HttpResponse<String> response=http.send(request,HttpResponse.BodyHandlers.ofString());
        if(response.statusCode()!=200) throw new IllegalStateException("Schema Registry lookup failed for schema ID "+schemaId+" with HTTP "+response.statusCode());
        return mapper.readTree(response.body());
    }
    private static String offsets(Map<TopicPartition,Long> offsets) {
        StringJoiner join=new StringJoiner(",");
        offsets.entrySet().stream().sorted(Comparator.comparing(e->e.getKey().partition()))
            .forEach(e->join.add(e.getKey().partition()+":"+e.getValue()));
        return join.toString();
    }
    private static String required(Map<String,String> options,String key) {
        String value=options.get(key);
        if(value==null || value.isBlank()) throw new IllegalArgumentException("Missing option: "+key);
        return value;
    }
}
