package com.sbase.platform.ingestion;

import org.junit.jupiter.api.Test;
import java.sql.SQLException;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class JdbcIngestionConnectorTenantIsolationTest {
    private final UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private final UUID tenantB=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test
    void acceptsOnlyRowsOwnedByTheConnectorTenant() throws Exception {
        JdbcIngestionConnector.requireTenantRow(tenantA,tenantA.toString());
        JdbcIngestionConnector.requireTenantRow(tenantA,tenantA);
        assertThrows(SQLException.class,()->JdbcIngestionConnector.requireTenantRow(tenantA,tenantB));
        assertThrows(SQLException.class,()->JdbcIngestionConnector.requireTenantRow(tenantA,null));
        assertThrows(SQLException.class,()->JdbcIngestionConnector.requireTenantRow(null,tenantA));
    }
}
