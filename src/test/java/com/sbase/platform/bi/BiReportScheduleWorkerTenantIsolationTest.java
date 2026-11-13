package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Executor;
import static org.mockito.Mockito.*;

class BiReportScheduleWorkerTenantIsolationTest {
    @Test
    void legacyScheduledReportExecutionPassesPersistedTenantIntoDorisQueryBoundary() throws Exception {
        var db=mock(org.springframework.jdbc.core.JdbcTemplate.class);
        var query=mock(DorisBiQueryService.class);
        Executor executor=Runnable::run;
        var worker=new BiReportScheduleWorker(db,query,new ObjectMapper(),executor);
        UUID tenant=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        String definition="""
            {"table":"orders","dimensions":["region"],
             "metrics":[{"column":"amount","alias":"total","function":"SUM"}],
             "filters":[],"limit":25}
            """;
        Method run=BiReportScheduleWorker.class.getDeclaredMethod("runQuery",UUID.class,String.class);
        run.setAccessible(true);
        run.invoke(worker,tenant,definition);
        verify(query).query(eq(tenant),eq("SELECT region,SUM(amount) AS total FROM orders GROUP BY region LIMIT 25"));
        verify(query,never()).query(anyString());
    }
}
