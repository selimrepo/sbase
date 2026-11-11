package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;

@Component
public class DorisIngestionConnector extends JdbcIngestionConnector implements IngestionConnector {
    public DorisIngestionConnector(DurableRecordSink checkpoints) {
        super(checkpoints);
    }

    @Override
    public String type() {
        return "doris";
    }
}
