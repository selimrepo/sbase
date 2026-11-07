package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;

@Component
public class KafkaIngestionConnector implements IngestionConnector {
    @Override
    public String type() {
        return "kafka";
    }

    @Override
    public ConnectorResult ingest(ConnectorRequest request) {
        // A consumer without a durable sink would either lose records or have to
        // replay them forever. Refuse execution until sink + offset-commit handling exists.
        throw new UnsupportedOperationException(
                "Kafka ingestion requires a configured durable sink; refusing to consume or acknowledge records");
    }
}
