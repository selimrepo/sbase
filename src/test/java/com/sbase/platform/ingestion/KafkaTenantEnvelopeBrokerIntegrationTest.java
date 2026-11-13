package com.sbase.platform.ingestion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Exercises tenant envelope checks against a real Kafka broker rather than only
 * mocked Headers instances. This verifies broker serialization does not change
 * the trusted header multiplicity or value seen by the consumer.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaTenantEnvelopeBrokerIntegrationTest {
    private static final UUID TENANT_A = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TENANT_B = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @Test
    void brokerPreservesTenantHeaderAndConsumerRejectsForeignOrDuplicatedHeaders() throws Exception {
        String topic = "tenant-envelope-" + UUID.randomUUID();
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(record(topic, "valid-a", TENANT_A.toString(), false)).get(10, TimeUnit.SECONDS);
            producer.send(record(topic, "foreign-b", TENANT_B.toString(), false)).get(10, TimeUnit.SECONDS);
            producer.send(record(topic, "duplicate-a", TENANT_A.toString(), true)).get(10, TimeUnit.SECONDS);
            producer.flush();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "tenant-envelope-test-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Set<String> accepted = new HashSet<>();
        Set<String> rejected = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (accepted.size() + rejected.size() < 3 && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    try {
                        TenantEnvelopeValidator.requireTrustedHeader(record.headers(), "sbase-tenant-id", TENANT_A);
                        accepted.add(record.key());
                    } catch (SecurityException denied) {
                        rejected.add(record.key());
                    }
                }
            }
        }

        assertEquals(Set.of("valid-a"), accepted);
        assertEquals(Set.of("foreign-b", "duplicate-a"), rejected);

        UUID connectorId = UUID.randomUUID();
        assertNotEquals(
                TenantEnvelopeValidator.tenantScopedConsumerGroup("shared", TENANT_A, connectorId),
                TenantEnvelopeValidator.tenantScopedConsumerGroup("shared", TENANT_B, connectorId));
    }

    private static ProducerRecord<String, String> record(
            String topic, String key, String tenant, boolean duplicate) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("sbase-tenant-id", tenant.getBytes(StandardCharsets.UTF_8));
        if (duplicate) {
            headers.add("sbase-tenant-id", tenant.getBytes(StandardCharsets.UTF_8));
        }
        return new ProducerRecord<>(topic, null, null, key, "{\"tenant_id\":\"" + tenant + "\"}", headers);
    }
}
