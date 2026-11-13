package com.sbase.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises an actual Kafka Connect worker and Debezium PostgreSQL source connector.
 * PostgreSQL WAL changes are emitted by Debezium, enriched with the row tenant ID
 * as a Kafka header, consumed from the real broker, and passed through SBase's
 * tenant validator and durable sink.
 */
@Testcontainers(disabledWithoutDocker = true)
class DebeziumConnectRuntimeIntegrationTest {
    private static final UUID TENANT_A = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TENANT_B = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    static final PostgreSQLContainer<?> SOURCE = new PostgreSQLContainer<>("postgres:16-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("cdc-postgres")
            .withCommand("postgres", "-c", "wal_level=logical",
                    "-c", "max_wal_senders=10", "-c", "max_replication_slots=10");

    @Container
    static final GenericContainer<?> CONNECT = new GenericContainer<>(
            DockerImageName.parse("quay.io/debezium/connect:2.7.3.Final"))
            .withNetwork(NETWORK)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("GROUP_ID", "sbase-debezium-connect-runtime")
            .withEnv("CONFIG_STORAGE_TOPIC", "sbase-connect-configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "sbase-connect-offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "sbase-connect-status")
            .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .waitingFor(Wait.forHttp("/").forPort(8083).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private HikariDataSource sinkDataSource;
    private JdbcTemplate sinkDb;
    private DurableRecordSink sink;
    private String topic;
    private String connectorName;

    @BeforeEach
    void prepareSink() {
        sinkDataSource = new HikariDataSource();
        sinkDataSource.setJdbcUrl(SOURCE.getJdbcUrl());
        sinkDataSource.setUsername(SOURCE.getUsername());
        sinkDataSource.setPassword(SOURCE.getPassword());
        // Use a separate database for the SBase sink so the source CDC table is not
        // accidentally consumed as an ingestion output table.
        sinkDb = new JdbcTemplate(sinkDataSource);
        sinkDb.execute("CREATE TABLE IF NOT EXISTS ingestion_connectors(id UUID PRIMARY KEY,tenant_id UUID NOT NULL)");
        sinkDb.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_partition_offsets(
                  tenant_id UUID NOT NULL, connector_id UUID NOT NULL, topic VARCHAR(249) NOT NULL,
                  partition_id INT NOT NULL, next_offset BIGINT NOT NULL, fencing_token BIGINT NOT NULL DEFAULT 0,
                  lease_owner VARCHAR(160), lease_until TIMESTAMPTZ,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY(tenant_id,connector_id,topic,partition_id))
                """);
        sinkDb.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_batch_receipts(
                  tenant_id UUID NOT NULL, connector_id UUID NOT NULL, topic VARCHAR(249) NOT NULL,
                  partition_id INT NOT NULL, first_offset BIGINT NOT NULL, next_offset BIGINT NOT NULL,
                  record_count INT NOT NULL,
                  PRIMARY KEY(tenant_id,connector_id,topic,partition_id,first_offset,next_offset))
                """);
        sinkDb.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_records(
                  id UUID PRIMARY KEY, connector_id UUID NOT NULL, tenant_id UUID NOT NULL,
                  source_key TEXT NOT NULL, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL,
                  UNIQUE(connector_id,source_key))
                """);
        sinkDb.execute("TRUNCATE ingestion_records,ingestion_batch_receipts,ingestion_partition_offsets,ingestion_connectors");
        sink = new DurableRecordSink(sinkDb,
                new TransactionTemplate(new DataSourceTransactionManager(sinkDataSource)), mapper);
        topic = "sbase-cdc-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        connectorName = "sbase-postgres-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    @AfterEach
    void closeSink() {
        if (sinkDataSource != null) sinkDataSource.close();
    }

    @Test
    void realConnectWorkerStreamsPostgresWalAndSbaseEnforcesTenantBoundary() throws Exception {
        SOURCE.execInContainer("sh", "-c", """
                psql -U %s -d %s -v ON_ERROR_STOP=1 -c
                "CREATE TABLE public.cdc_events(id UUID PRIMARY KEY, tenant_id UUID NOT NULL, message TEXT NOT NULL)"
                """.formatted(SOURCE.getUsername(), SOURCE.getDatabaseName()));

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", "cdc-postgres");
        config.put("database.port", "5432");
        config.put("database.user", SOURCE.getUsername());
        config.put("database.password", SOURCE.getPassword());
        config.put("database.dbname", SOURCE.getDatabaseName());
        config.put("topic.prefix", topic);
        config.put("plugin.name", "pgoutput");
        config.put("slot.name", "sbase_" + connectorName.replace("-", "_"));
        config.put("publication.name", "sbase_pub_" + connectorName.replace("-", "_"));
        config.put("publication.autocreate.mode", "filtered");
        config.put("table.include.list", "public.cdc_events");
        config.put("snapshot.mode", "initial");
        config.put("include.schema.changes", "false");
        config.put("tombstones.on.delete", "false");
        config.put("transforms", "unwrap,tenantHeader");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.drop.tombstones", "false");
        config.put("transforms.unwrap.delete.handling.mode", "none");
        config.put("transforms.tenantHeader.type", "org.apache.kafka.connect.transforms.HeaderFrom$Value");
        config.put("transforms.tenantHeader.fields", "tenant_id");
        config.put("transforms.tenantHeader.headers", "sbase-tenant-id");
        config.put("transforms.tenantHeader.operation", "copy");

        Map<String, Object> request = Map.of("name", connectorName, "config", config);
        HttpResponse<String> created = http.send(HttpRequest.newBuilder(connectUrl("/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertTrue(created.statusCode() == 201 || created.statusCode() == 409,
                "Kafka Connect connector registration failed: " + created.statusCode() + " " + created.body());

        awaitConnectorRunning();

        String table = SOURCE.getDatabaseName();
        String insert = """
                INSERT INTO public.cdc_events(id,tenant_id,message) VALUES
                ('10000000-0000-0000-0000-000000000001','%s','tenant-a-cdc'),
                ('20000000-0000-0000-0000-000000000002','%s','tenant-b-cdc')
                """.formatted(TENANT_A, TENANT_B);
        SOURCE.execInContainer("sh", "-c", "psql -U " + SOURCE.getUsername() + " -d " + table
                + " -v ON_ERROR_STOP=1 -c \"" + insert.replace("\n", " ") + "\"");

        UUID connectorA = createSinkConnector(TENANT_A);
        UUID connectorB = createSinkConnector(TENANT_B);
        List<ConsumerRecord<String, String>> records = consumeTwoCdcEvents();
        assertEquals(2, records.size(), "Debezium should emit both committed PostgreSQL row changes");

        int acceptedA = 0;
        int acceptedB = 0;
        for (ConsumerRecord<String, String> record : records) {
            String tenantHeader = singleHeader(record, "sbase-tenant-id");
            UUID rowTenant = UUID.fromString(tenantHeader);
            TenantEnvelopeValidator.requireTrustedHeader(record.headers(), "sbase-tenant-id", rowTenant);
            TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper, record.value(), rowTenant);

            Map<String, Object> row = mapper.readValue(record.value(), new TypeReference<>() {});
            assertEquals(rowTenant.toString(), String.valueOf(row.get("tenant_id")));
            if (TENANT_A.equals(rowTenant)) {
                assertThrows(SecurityException.class, () ->
                        TenantEnvelopeValidator.requireTrustedHeader(record.headers(), "sbase-tenant-id", TENANT_B));
                assertThrows(SecurityException.class, () ->
                        TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper, record.value(), TENANT_B));
                persist(record, connectorA, TENANT_A);
                acceptedA++;
            } else if (TENANT_B.equals(rowTenant)) {
                assertThrows(SecurityException.class, () ->
                        TenantEnvelopeValidator.requireTrustedHeader(record.headers(), "sbase-tenant-id", TENANT_A));
                assertThrows(SecurityException.class, () ->
                        TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper, record.value(), TENANT_A));
                persist(record, connectorB, TENANT_B);
                acceptedB++;
            } else {
                fail("Unexpected tenant emitted by Debezium: " + rowTenant);
            }
        }

        assertEquals(1, acceptedA);
        assertEquals(1, acceptedB);
        assertEquals(1, count(connectorA));
        assertEquals(1, count(connectorB));
        assertEquals(1, sinkDb.queryForObject(
                "SELECT count(*) FROM ingestion_records WHERE tenant_id=?", Integer.class, TENANT_A));
        assertEquals(1, sinkDb.queryForObject(
                "SELECT count(*) FROM ingestion_records WHERE tenant_id=?", Integer.class, TENANT_B));
    }

    private void persist(ConsumerRecord<String, String> record, UUID connectorId, UUID tenantId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("tenant_id", tenantId.toString());
        payload.put("topic", record.topic());
        payload.put("partition", record.partition());
        payload.put("offset", record.offset());
        payload.put("debezium_record", record.value());
        payload.put("debezium_key", record.key());
        DurableRecordSink.Envelope envelope = new DurableRecordSink.Envelope(
                record.topic() + ":" + record.partition() + ":" + record.offset(), payload);
        long token = sink.claimKafkaPartition(connectorId, record.topic(), record.partition(),
                "connect-runtime-" + tenantId);
        assertEquals(1, sink.writeKafkaBatch(connectorId, record.topic(), List.of(envelope),
                "connect-runtime-" + tenantId, Map.of(record.partition(), token)));
    }

    private UUID createSinkConnector(UUID tenantId) {
        UUID id = UUID.randomUUID();
        sinkDb.update("INSERT INTO ingestion_connectors(id,tenant_id) VALUES(?,?)", id, tenantId);
        return id;
    }

    private long count(UUID connectorId) {
        return sinkDb.queryForObject("SELECT count(*) FROM ingestion_records WHERE connector_id=?",
                Long.class, connectorId);
    }

    private List<ConsumerRecord<String, String>> consumeTwoCdcEvents() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sbase-runtime-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (result.size() < 2 && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    result.add(record);
                }
            }
        }
        assertEquals(2, result.size(), "Timed out waiting for real Debezium WAL events");
        return result;
    }

    private void awaitConnectorRunning() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        String last = "no status response";
        while (System.nanoTime() < deadline) {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                    connectUrl("/connectors/" + connectorName + "/status")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            last = response.body();
            if (response.statusCode() == 200) {
                Map<String, Object> status = mapper.readValue(last, new TypeReference<>() {});
                Map<?, ?> connector = (Map<?, ?>) status.get("connector");
                if ("RUNNING".equals(connector.get("state"))) return;
                if ("FAILED".equals(connector.get("state"))) {
                    fail("Debezium PostgreSQL connector failed: " + last);
                }
            }
            Thread.sleep(1000);
        }
        fail("Debezium connector did not reach RUNNING state: " + last);
    }

    private URI connectUrl(String path) {
        return URI.create("http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path);
    }

    private static String singleHeader(ConsumerRecord<String, String> record, String name) {
        Iterator<Header> headers = record.headers().headers(name).iterator();
        assertTrue(headers.hasNext(), "Debezium record is missing tenant header " + name);
        String value = new String(headers.next().value(), StandardCharsets.UTF_8);
        assertFalse(headers.hasNext(), "Debezium record has duplicate tenant headers");
        return value;
    }
}
