package io.sbase.server.engines;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class FederatedQueryPlannerTest {
    @Test void choosesCrossSourceExchangeAndSmallestFirstScanOrder() throws Exception {
        var catalog=Map.of(
          "customers",Map.<String,Object>of("source","postgresql","rows",100L,"bytes",12000L,"partitions",2),
          "orders",Map.<String,Object>of("source","cassandra","rows",100000L,"bytes",9000000L,"partitions",16)
        );
        var plan=new FederatedQueryPlanner().plan(
          "SELECT c.id, o.total FROM customers c JOIN orders o ON c.id = o.customer_id",catalog);
        assertEquals(Boolean.TRUE,plan.get("crossSourceJoin"));
        assertEquals("customers",((Map<?,?>)((java.util.List<?>)plan.get("scans")).get(0)).get("table"));
        assertEquals("BROADCAST_SMALL_SIDE_OR_HASH_SHUFFLE",((Map<?,?>)plan.get("joinStrategy")).get("type"));
    }

    @Test void rejectsMutatingSql() {
        assertThrows(Exception.class,()->new FederatedQueryPlanner().plan("DELETE FROM customers",Map.of()));
    }
}
