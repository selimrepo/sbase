package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Consumes Debezium Kafka Connect topics and persists the complete change envelope before committing offsets. */
@Component
public class DebeziumIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink;
    private final ObjectMapper mapper;

    public DebeziumIngestionConnector(DurableRecordSink sink, ObjectMapper mapper) {
        this.sink = sink;
        this.mapper = mapper;
    }

    @Override
    public String type() {
        return "debezium";
    }

    @Override
    public ConnectorResult ingest(ConnectorRequest request) {
        Map<String, String> options = request.options();
        UUID connectorId = UUID.fromString(required(options, "connectorId"));
        UUID tenantId = UUID.fromString(required(options, "tenantId"));
        String tenantHeader = options.getOrDefault("tenantHeader", "sbase-tenant-id");
        String topic = required(options, "topic");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, required(options, "bootstrapServers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TenantEnvelopeValidator.tenantScopedConsumerGroup(
                options.getOrDefault("groupId", "sbase-cdc"), tenantId, connectorId));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getOrDefault("autoOffsetReset", "earliest"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int maxRecords = Integer.parseInt(options.getOrDefault("maxRecords", "500"));
        long pollTimeoutMs = Long.parseLong(options.getOrDefault("pollTimeoutMs", "5000"));
        if (maxRecords < 1 || maxRecords > 10000 || pollTimeoutMs < 1 || pollTimeoutMs > 300000) {
            throw new IllegalArgumentException("maxRecords must be 1..10000 and pollTimeoutMs must be 1..300000");
        }

        long read = 0;
        long written = 0;
        long deadline = System.currentTimeMillis() + pollTimeoutMs;
        long lastLeaseRenewal = 0;
        String ownerId = UUID.randomUUID().toString();
        Map<Integer, Long> partitionTokens = new ConcurrentHashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    release(partitions);
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    release(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition partition : partitions) {
                        long token = sink.claimKafkaPartition(connectorId, topic, partition.partition(), ownerId);
                        partitionTokens.put(partition.partition(), token);
                    }
                }

                private void release(Collection<TopicPartition> partitions) {
                    for (TopicPartition partition : partitions) {
                        Long token = partitionTokens.remove(partition.partition());
                        if (token != null) {
                            sink.releaseKafkaPartition(connectorId, topic, partition.partition(), ownerId, token);
                        }
                    }
                }
            });

            while (read < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();
                if (now - lastLeaseRenewal >= 30000) {
                    for (Map.Entry<Integer, Long> lease : partitionTokens.entrySet()) {
                        if (!sink.renewKafkaPartition(connectorId, topic, lease.getKey(), ownerId, lease.getValue())) {
                            throw new IllegalStateException("Debezium partition lease expired or was fenced: " + lease.getKey());
                        }
                    }
                    lastLeaseRenewal = now;
                }
                if (records.isEmpty()) {
                    continue;
                }

                List<DurableRecordSink.Envelope> batch = new ArrayList<>();
                Map<TopicPartition, Long> nextOffsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    TenantEnvelopeValidator.requireTrustedHeader(record.headers(), tenantHeader, tenantId);
                    TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper, record.value(), tenantId);
                    String sourceKey = topic + ":" + record.partition() + ":" + record.offset();
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("tenant_id", tenantId.toString());
                    payload.put("key", record.key());
                    payload.put("envelope", record.value());
                    payload.put("topic", topic);
                    payload.put("partition", record.partition());
                    payload.put("offset", record.offset());
                    // Preserve Debezium before/after/op/source/ts_ms without lossy flattening.
                    batch.add(new DurableRecordSink.Envelope(sourceKey, payload));
                    nextOffsets.put(new TopicPartition(topic, record.partition()), record.offset() + 1);
                }

                Map<Integer, Long> batchTokens = new HashMap<>();
                for (TopicPartition partition : nextOffsets.keySet()) {
                    Long token = partitionTokens.get(partition.partition());
                    if (token == null) {
                        throw new IllegalStateException("Debezium partition is no longer owned by this consumer: " + partition);
                    }
                    batchTokens.put(partition.partition(), token);
                }

                // Fencing, receipts, records and next offsets commit in one PostgreSQL transaction.
                written += sink.writeKafkaBatch(connectorId, topic, batch, ownerId, batchTokens);
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                nextOffsets.forEach((partition, offset) -> commits.put(partition, new OffsetAndMetadata(offset)));
                consumer.commitSync(commits);
                read += records.count();
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Debezium CDC persistence failed before offset acknowledgement", exception);
        }

        return new ConnectorResult(read, written,
                "Debezium change envelopes persisted with tenant validation and PostgreSQL partition fencing; offsets committed after sink transaction");
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing option: " + key);
        }
        return value;
    }
}
