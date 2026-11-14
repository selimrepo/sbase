package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import com.sbase.platform.bi.PostgreSQLTenantTablePolicy;
import com.sbase.postgres.PostgreSQLClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PostgreSQLIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
    private final PostgreSQLTenantTablePolicy tablePolicy;
    private final PostgreSQLClient postgres;

    public PostgreSQLIngestionConnector(DurableRecordSink checkpoints,PostgreSQLTenantTablePolicy tablePolicy,PostgreSQLClient postgres) {
        super(checkpoints);
        this.tablePolicy=tablePolicy;
        this.postgres=postgres;
    }

    @Override protected String validateTargetTable(String table) {
        return tablePolicy.requireTenantScoped(table);
    }

    @Override protected Connection openTargetConnection(Map<String,String> options) throws SQLException {
        return postgres.openConnection();
    }

    @Override
    public String type() {
        return "postgresql";
    }
}
