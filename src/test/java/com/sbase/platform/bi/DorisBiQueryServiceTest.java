package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DorisBiQueryServiceTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final DorisBiQueryService service = new DorisBiQueryService("analytics.orders", jdbc);

    @Test
    void addsDefaultLimitToAllowListedSelect() {
        when(jdbc.queryForList("SELECT id FROM analytics.orders LIMIT 1000"))
                .thenReturn(List.of(Map.of("id", 1)));
        assertEquals(1, service.query("SELECT id FROM analytics.orders").size());
        verify(jdbc).queryForList("SELECT id FROM analytics.orders LIMIT 1000");
    }

    @Test
    void rejectsUnionAndSubqueries() {
        assertThrows(IllegalArgumentException.class,
                () -> service.query("SELECT id FROM analytics.orders UNION SELECT id FROM secrets"));
        assertThrows(IllegalArgumentException.class,
                () -> service.query("SELECT id FROM analytics.orders WHERE id IN (SELECT id FROM secrets)"));
        verifyNoInteractions(jdbc);
    }

    @Test
    void rejectsUnlistedTablesAndExcessiveLimits() {
        assertThrows(IllegalArgumentException.class, () -> service.query("SELECT id FROM secrets"));
        assertThrows(IllegalArgumentException.class,
                () -> service.query("SELECT id FROM analytics.orders LIMIT 10001"));
        verifyNoInteractions(jdbc);
    }

    @Test
    void allowsExplicitBoundedLimit() {
        when(jdbc.queryForList("SELECT id FROM analytics.orders LIMIT 25"))
                .thenReturn(List.of(Map.of("id", 1)));
        assertEquals(1, service.query("SELECT id FROM analytics.orders LIMIT 25").size());
    }
}
