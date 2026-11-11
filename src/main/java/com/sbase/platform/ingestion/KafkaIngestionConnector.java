package com.sbase.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;

@Component
public class KafkaIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink;
    private final ObjectMapper mapper;

    public KafkaIngestionConnector(DurableRecordSink sink, ObjectMapper mapper) {
        this.sink = sink; this.mapper = mapper;
    }
    @Override public String type() { return "kafka"; }

    @Override public ConnectorResult ingest(ConnectorRequest request) {
        Map<String,String> o = request.options();
        UUID connectorId = UUID.fromString(required(o, "connectorId"));
        String topic = required(o, "topic");
        String bootstrap = required(o, "bootstrapServers");
        String group = o.getOrDefault("groupId", "sbase-" + connectorId);
        int max = Integer.parseInt(o.getOrDefault("maxRecords", "500"));
        if (max < 1 || max > 10000) throw new IllegalArgumentException("maxRecords must be between 1 and 10000");
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, o.getOrDefault("autoOffsetReset", "earliest"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max);
        long read=0, written=0;
        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + Long.parseLong(o.getOrDefault("pollTimeoutMs","5000"));
            while (read < max && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String,String> batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) continue;
                List<DurableRecordSink.Envelope> envelopes = new ArrayList<>();
                Map<TopicPartition,Long> nextOffsets = new HashMap<>();
                for (ConsumerRecord<String,String> r : batch) {
                    String key = topic + ":" + r.partition() + ":" + r.offset();
                    Map<String,Object> payload = new LinkedHashMap<>();
                    payload.put("topic",topic); payload.put("partition",r.partition()); payload.put("offset",r.offset());
                    payload.put("key",r.key()); payload.put("value",r.value()); payload.put("timestamp",r.timestamp());
                    envelopes.add(new DurableRecordSink.Envelope(key,payload));
                    nextOffsets.put(new TopicPartition(topic,r.partition()),r.offset()+1);
                }
                written += sink.write(connectorId, topic, envelopes, "kafka:"+topic, offsets(nextOffsets));
                // Commit only after the PostgreSQL transaction containing payloads and checkpoint succeeded.
                Map<TopicPartition,OffsetAndMetadata> commits = new HashMap<>();
                nextOffsets.forEach((tp,off)->commits.put(tp,new OffsetAndMetadata(off)));
                consumer.commitSync(commits);
                read += batch.count();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Kafka durable ingestion failed; offsets were not acknowledged before sink persistence",e);
        }
        return new ConnectorResult(read,written,"Persisted to PostgreSQL ingestion_records; Kafka offsets committed after durable sink transaction");
    }
    private static String offsets(Map<TopicPartition,Long> offsets) {
        StringJoiner j=new StringJoiner(",");
        offsets.entrySet().stream().sorted(Comparator.comparing(e->e.getKey().partition()))
            .forEach(e->j.add(e.getKey().partition()+":"+e.getValue()));
        return j.toString();
    }
    private static String required(Map<String,String> o,String k) {
        String v=o.get(k); if(v==null||v.isBlank()) throw new IllegalArgumentException("Missing option: "+k); return v;
    }
}
