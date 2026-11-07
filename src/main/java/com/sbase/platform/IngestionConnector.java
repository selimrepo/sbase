package com.sbase.platform;
import java.util.Map;
/** SPI for pluggable source-to-target ingestion implementations. */
public interface IngestionConnector {
 String type();
 ConnectorResult ingest(ConnectorRequest request);
 record ConnectorRequest(String source,String target,Map<String,String> options){}
 record ConnectorResult(long recordsRead,long recordsWritten,String details){}
}