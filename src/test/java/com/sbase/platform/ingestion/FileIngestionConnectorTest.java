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
    void parsesQuotedCommasEscapedQuotesAndEmbeddedNewlines() throws Exception {
        DurableRecordSink sink=mock(DurableRecordSink.class);
        when(sink.write(any(),anyString(),anyList(),anyString(),anyString())).thenReturn(1L);
        FileIngestionConnector connector=new FileIngestionConnector(sink,new ObjectMapper());
        Path file=tempDir.resolve("customers.csv");
        Files.writeString(file,"name,comment\r\n\"Doe, Jane\",\"said \"\"hello\"\"\"\r\n\"Multi line\",\"first line\r\nsecond line\"\r\n");
        UUID connectorId=UUID.randomUUID();
        var options=new HashMap<String,String>();
        options.put("connectorId",connectorId.toString());
        options.put("path",file.toString());
        options.put("allowedRoot",tempDir.toString());
        options.put("format","csv");
        IngestionConnector.ConnectorResult result=connector.ingest(new IngestionConnector.ConnectorRequest(null,null,options));

        assertEquals(2,result.recordsRead());
        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<List<DurableRecordSink.Envelope>> captor=org.mockito.ArgumentCaptor.forClass(List.class);
        verify(sink).write(eq(connectorId),eq(file.toRealPath().toString()),captor.capture(),eq("file:"+file.toRealPath()),anyString());
        List<DurableRecordSink.Envelope> rows=captor.getValue();
        assertEquals("Doe, Jane",rows.get(0).payload().get("name"));
        assertEquals("said \"hello\"",rows.get(0).payload().get("comment"));
        assertEquals("first line\r\nsecond line",rows.get(1).payload().get("comment"));
    }
}
