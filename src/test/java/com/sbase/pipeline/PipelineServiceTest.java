package com.sbase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PipelineServiceTest {
    private final PipelineService service=new PipelineService(null,new ObjectMapper());

    @Test void acceptsExecutableDorisPipeline(){
        assertDoesNotThrow(()->service.validateDefinition("""
          {"steps":[
            {"type":"read-doris","table":"analytics.events"},
            {"type":"filter","column":"amount","operator":"gte","value":10},
            {"type":"select","columns":["customer_id","amount"]},
            {"type":"write-parquet","uri":"s3a://warehouse/curated/events"}
          ]}
        """));
    }
    @Test void acceptsParquetFeaturePipeline(){
        assertDoesNotThrow(()->service.validateDefinition("""
          {"steps":[
            {"type":"read-parquet","uri":"s3a://warehouse/raw/events"},
            {"type":"derive","operation":"cast-double","source":"amount","output":"amount_value"},
            {"type":"aggregate","groupBy":["country"],"metrics":[{"function":"sum","column":"amount_value","alias":"revenue"}]},
            {"type":"write-doris","table":"analytics.country_revenue"}
          ]}
        """));
    }
    @Test void rejectsMissingSourceOrSink(){
        assertThrows(Exception.class,()->service.validateDefinition("{\"steps\":[{\"type\":\"select\",\"columns\":[\"id\"]}]}"));
    }
    @Test void rejectsUnknownStepTypes(){
        assertThrows(Exception.class,()->service.validateDefinition("{\"steps\":[{\"type\":\"read-doris\",\"table\":\"a.b\"},{\"type\":\"run-shell\"},{\"type\":\"write-doris\",\"table\":\"a.c\"}]}"));
    }
    @Test void rejectsNonTerminalWrite(){
        assertThrows(Exception.class,()->service.validateDefinition("{\"steps\":[{\"type\":\"read-doris\",\"table\":\"a.b\"},{\"type\":\"write-doris\",\"table\":\"a.c\"},{\"type\":\"select\",\"columns\":[\"id\"]}]}"));
    }
}
