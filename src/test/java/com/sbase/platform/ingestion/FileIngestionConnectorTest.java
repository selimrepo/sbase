package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FileIngestionConnectorTest {
    @TempDir Path tempDir;

    @Test
    void parsesCsvAndPersistsOnlyWithinTenantDirectory() throws Exception {
        DurableRecordSink sink=mock(DurableRecordSink.class);
        when(sink.write(any(),anyString(),anyList(),anyString(),anyString())).thenReturn(2L);
        UUID tenant=UUID.randomUUID(),connectorId=UUID.randomUUID();
        Path tenantRoot=tempDir.resolve("tenants").resolve(tenant.toString());
        Files.createDirectories(tenantRoot);
        Path file=tenantRoot.resolve("customers.csv");
        Files.writeString(file,"tenant_id,name,comment\r\n"+tenant+",\"Doe, Jane\",\"said \"\"hello\"\"\"\r\n"+
            tenant+",\"Multi line\",\"first line\r\nsecond line\"\r\n");
        FileIngestionConnector connector=new FileIngestionConnector(sink,new ObjectMapper(),tempDir.toString());
        var options=new HashMap<String,String>();
        options.put("connectorId",connectorId.toString());
        options.put("tenantId",tenant.toString());
        options.put("path",file.toString());
        options.put("format","csv");
        IngestionConnector.ConnectorResult result=connector.ingest(new IngestionConnector.ConnectorRequest(null,null,options));

        assertEquals(2,result.recordsRead());
        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<List<DurableRecordSink.Envelope>> captor=org.mockito.ArgumentCaptor.forClass(List.class);
        verify(sink).write(eq(connectorId),eq(file.toRealPath().toString()),captor.capture(),eq("file:"+file.toRealPath()),anyString());
        List<DurableRecordSink.Envelope> rows=captor.getValue();
        assertEquals(tenant.toString(),rows.get(0).payload().get("tenant_id"));
        assertEquals("Doe, Jane",rows.get(0).payload().get("name"));
        assertEquals("said \"hello\"",rows.get(0).payload().get("comment"));
        assertEquals("first line\r\nsecond line",rows.get(1).payload().get("comment"));
    }

    @Test
    void cannotReadAnotherTenantsDirectory() throws Exception {
        DurableRecordSink sink=mock(DurableRecordSink.class);
        UUID tenantA=UUID.randomUUID(),tenantB=UUID.randomUUID();
        Path foreignRoot=tempDir.resolve("tenants").resolve(tenantB.toString());
        Files.createDirectories(foreignRoot);
        Path foreign=foreignRoot.resolve("secret.jsonl");
        Files.writeString(foreign,"{\\\"tenant_id\\\":\\\""+tenantB+"\\\"}");
        FileIngestionConnector connector=new FileIngestionConnector(sink,new ObjectMapper(),tempDir.toString());
        var options=new HashMap<String,String>();
        options.put("connectorId",UUID.randomUUID().toString());
        options.put("tenantId",tenantA.toString());
        options.put("path",foreign.toString());
        options.put("format","jsonl");
        assertThrows(IllegalArgumentException.class,()->connector.ingest(new IngestionConnector.ConnectorRequest(null,null,options)));
        verifyNoInteractions(sink);
    }
}
