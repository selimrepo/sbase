package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import com.sbase.platform.bi.DorisTenantTablePolicy;
import com.sbase.doris.DorisClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DorisIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
    private final DorisTenantTablePolicy tablePolicy;
    private final DorisClient doris;

    public DorisIngestionConnector(DurableRecordSink checkpoints,DorisTenantTablePolicy tablePolicy,DorisClient doris) {
        super(checkpoints);
        this.tablePolicy=tablePolicy;
        this.doris=doris;
    }

    @Override protected String validateTargetTable(String table) {
        return tablePolicy.requireTenantScoped(table);
    }

    @Override protected Connection openTargetConnection(Map<String,String> options) throws SQLException {
        return doris.openConnection();
    }

    @Override
    public String type() {
        return "doris";
    }
}
