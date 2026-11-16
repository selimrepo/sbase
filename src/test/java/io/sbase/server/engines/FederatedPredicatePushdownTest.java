package io.sbase.server.engines;

import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class FederatedPredicatePushdownTest {
    @Test
    void translatesSingleTableSqlToPostgresAndCassandra() {
        String sql = "SELECT customer_id, amount FROM orders WHERE amount >= 100 AND region = 'EU'";
        assertEquals(Optional.of("SELECT customer_id, amount FROM orders WHERE (amount >= 100 AND region = 'EU') LIMIT 250"),
                FederatedPredicatePushdown.translate(sql, "postgresql", "orders", 250));
        assertEquals(Optional.of("SELECT customer_id, amount FROM orders WHERE (amount >= 100 AND region = 'EU') LIMIT 250"),
                FederatedPredicatePushdown.translate(sql, "cassandra", "orders", 250));
    }

    @Test
    void translatesSolrPredicateAndRejectsJoinPushdown() {
        assertEquals(Optional.of("(category:\"books\" AND price:[20 TO *])"),
                FederatedPredicatePushdown.translate(
                        "SELECT id FROM products WHERE category = 'books' AND price >= 20",
                        "solr", null, 100));
        assertTrue(FederatedPredicatePushdown.translate(
                "SELECT p.id FROM products p JOIN stock s ON p.id = s.product_id WHERE p.active = TRUE",
                "postgresql", "products", 100).isEmpty());
    }

    @Test
    void rejectsUnsupportedExpressionsRatherThanChangingSemantics() {
        assertTrue(FederatedPredicatePushdown.translate(
                "SELECT LOWER(name) FROM products WHERE price + 1 > 10",
                "postgresql", "products", 100).isEmpty());
    }
}
