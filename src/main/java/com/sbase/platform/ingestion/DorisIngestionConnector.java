package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import com.sbase.platform.bi.DorisTenantTablePolicy;
import com.sbase.doris.DorisClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Legacy connector id retained for stored configurations; writes target PostgreSQL. */
@Component
public class DorisIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
    private final DorisTenantTablePolicy tablePolicy;
    private final DorisClient postgres;

    public DorisIngestionConnector(DurableRecordSink checkpoints, DorisTenantTablePolicy tablePolicy, DorisClient postgres) {
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
