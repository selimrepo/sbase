package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.pipeline.PipelineExecutionService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Executor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DistributedScheduleWorkerTenantIsolationTest {
    private final JdbcTemplate db=mock(JdbcTemplate.class);
    private final PipelineExecutionService pipelines=mock(PipelineExecutionService.class);
    private final Executor executor=Runnable::run;

    private DistributedScheduleWorker worker() {
        return new DistributedScheduleWorker(db,executor,pipelines,new ObjectMapper());
    }

    @Test
    void schedulerClaimsOnlySchedulesWhosePipelineHasTheSameTenant() {
        when(db.query(anyString(),any(RowMapper.class),any(),any(),any())).thenReturn(List.of());
        worker().claim();
        org.mockito.ArgumentCaptor<String> sql=org.mockito.ArgumentCaptor.forClass(String.class);
        verify(db).query(sql.capture(),any(RowMapper.class),eq(10),anyString(),eq(120L));
        assertTrue(sql.getValue().contains("LEFT JOIN pipelines p ON p.id = s.pipeline_id AND p.tenant_id = s.tenant_id"));
        assertTrue(sql.getValue().contains("parent.tenant_id = s.tenant_id"));
    }

    @Test
    void submittedRunReconciliationRequiresScheduleRunAndPipelineTenantToMatch() throws Exception {
        DistributedScheduleWorker worker=worker();
        Method reconcile=DistributedScheduleWorker.class.getDeclaredMethod("reconcileSubmittedRuns");
        reconcile.setAccessible(true);
        reconcile.invoke(worker);
        org.mockito.ArgumentCaptor<String> sql=org.mockito.ArgumentCaptor.forClass(String.class);
        verify(db).update(sql.capture());
        assertTrue(sql.getValue().contains("sr.tenant_id = pr.tenant_id"));
    }
}
