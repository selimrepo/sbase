package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConnectorExecutionServiceTenantIsolationTest {
    private static final String LOOKUP="SELECT connector_type FROM ingestion_connectors WHERE id=? AND tenant_id=? AND enabled=true";
    private final JdbcTemplate db=mock(JdbcTemplate.class);
    private final IngestionConnector connector=mock(IngestionConnector.class);
    private final UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private final UUID tenantB=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private final UUID connectorA=UUID.randomUUID();

    private ConnectorExecutionService service() {
        when(connector.type()).thenReturn("file");
        return new ConnectorExecutionService(List.of(connector),db,new ObjectMapper(),2,Runnable::run);
    }

    @Test
    void foreignTenantCannotEnqueueRunsForAnotherTenantsConnector() {
        when(db.query(eq(LOOKUP),any(ResultSetExtractor.class),eq(connectorA),eq(tenantB))).thenReturn(null);
        assertThrows(ResponseStatusException.class,()->service().execute("file",connectorA,null,
            "source","target",Map.of(),"key",tenantB,"bob"));
        verify(db).query(eq(LOOKUP),any(ResultSetExtractor.class),eq(connectorA),eq(tenantB));
        verify(db,never()).update(startsWith("INSERT INTO ingestion_runs"),any(Object[].class));
    }

    @Test
    void connectorLookupAlwaysUsesTheCallerTenantEvenForKnownConnectorIds() {
        when(db.query(eq(LOOKUP),any(ResultSetExtractor.class),eq(connectorA),eq(tenantA))).thenReturn(null);
        assertThrows(ResponseStatusException.class,()->service().execute("file",connectorA,null,
            "source","target",Map.of(),"key",tenantA,"alice"));
        verify(db).query(eq(LOOKUP),any(ResultSetExtractor.class),eq(connectorA),eq(tenantA));
        verify(db,never()).update(startsWith("INSERT INTO ingestion_runs"),any(Object[].class));
    }
}
