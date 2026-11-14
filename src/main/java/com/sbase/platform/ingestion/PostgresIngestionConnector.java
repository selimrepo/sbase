package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import com.sbase.platform.bi.PostgresTenantTablePolicy;
import com.sbase.postgres.PostgresClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Legacy connector id retained for stored configurations; writes target PostgreSQL. */
@Component
public class PostgresIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
    private final PostgresTenantTablePolicy tablePolicy;
    private final PostgresClient postgres;

    public PostgresIngestionConnector(DurableRecordSink checkpoints, PostgresTenantTablePolicy tablePolicy, PostgresClient postgres) {
        super(checkpoints);
        this.tablePolicy = tablePolicy;
        this.postgres = postgres;
    }

    @Override protected String validateTargetTable(String table) {
        return tablePolicy.requireTenantScoped(table);
    }

    @Override protected Connection openTargetConnection(Map<String,String> options) throws SQLException {
        return postgres.openConnection();
    }

    @Override public String type() { return "postgresql"; }
}
