package io.sbase.server.lakehouse;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FlinkSqlGuardTest {
    private static final String SOURCE="""
        CREATE TABLE orders_source (
          order_id STRING,
          amount DECIMAL(18,2)
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'orders',
          'properties.bootstrap.servers' = 'kafka:9092',
          'format' = 'json'
        )
        """;
    private static final String INSERT="INSERT INTO sbase_iceberg.analytics.orders SELECT order_id, amount FROM orders_source";

    @Test void acceptsKafkaSourceAndExactIcebergTarget() {
        assertDoesNotThrow(()->FlinkSqlGuard.validate("analytics","orders",SOURCE,INSERT));
    }

    @Test void rejectsCrossNamespaceOrCrossTableSink() {
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",SOURCE,
                        "INSERT INTO sbase_iceberg.payroll.salary SELECT * FROM orders_source"));
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",SOURCE,
                        "INSERT INTO sbase_iceberg.analytics.other SELECT * FROM orders_source"));
    }

    @Test void rejectsNonKafkaAndQualifiedSourceTableDefinitions() {
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",
                        SOURCE.replace("'kafka'","'filesystem'"),INSERT));
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",
                        SOURCE.replace("orders_source","sbase_iceberg.analytics.shadow"),INSERT));
    }

    @Test void rejectsMultipleStatementsCommentsAndEmbeddedCredentials() {
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",SOURCE+"; DROP TABLE x",INSERT));
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",SOURCE+" -- hidden",INSERT));
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",
                        SOURCE.replace("'topic'","'password'"),INSERT));
    }

    @Test void rejectsInvalidIdentifiersAndUnqualifiedSinks() {
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics;drop","orders",SOURCE,INSERT));
        assertThrows(IllegalArgumentException.class,
                ()->FlinkSqlGuard.validate("analytics","orders",SOURCE,
                        "INSERT INTO orders SELECT * FROM orders_source"));
    }
}
