package com.sbase.platform.feature;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OnlineFeatureStoreServiceTest {
    @Test
    void upsertUsesConflictSafeLatestEventSemantics() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        OnlineFeatureStoreService service = new OnlineFeatureStoreService(jdbc, new ObjectMapper());
        service.upsert("customer_profile", "customer-42", 3,
            Instant.parse("2026-11-08T10:00:00Z"), Map.of("lifetime_value", 123.5));
        verify(jdbc).update(argThat(sql -> sql.contains("ON CONFLICT(tenant_id,feature_set,entity_key) DO UPDATE") && sql.contains("EXCLUDED.event_time > online_feature_values.event_time")),
            eq(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")),
            eq("customer_profile"), eq("customer-42"), eq(3), anyString(), any(java.sql.Timestamp.class));
    }

    @Test
    void rejectsInvalidFeatureSetAndEmptyValues() {
        OnlineFeatureStoreService service =
            new OnlineFeatureStoreService(mock(JdbcTemplate.class), new ObjectMapper());
        assertThrows(IllegalArgumentException.class,
            () -> service.upsert("bad name", "entity-1", 1, Instant.now(), Map.of("x", 1)));
        assertThrows(IllegalArgumentException.class,
            () -> service.upsert("valid", "entity-1", 1, Instant.now(), Map.of()));
    }
}
