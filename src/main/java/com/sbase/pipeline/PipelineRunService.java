package com.sbase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

@Service
public class PipelineRunService {
    private final PipelineRepository pipelines;
    private final PipelineRunRepository runs;
    private final PipelineExecutionEngine engine;
    private final Executor executor;
    private final ObjectMapper mapper;

    public PipelineRunService(PipelineRepository pipelines,PipelineRunRepository runs,
                              PipelineExecutionEngine engine,@Qualifier("miningExecutor") Executor executor,
                              ObjectMapper mapper) {
        this.pipelines=pipelines; this.runs=runs; this.engine=engine; this.executor=executor; this.mapper=mapper;
    }

    @Transactional
    public PipelineRunEntity submit(UUID pipelineId) {
        PipelineEntity pipeline=enabledPipeline(pipelineId);
        PipelineRunEntity run=runs.saveAndFlush(new PipelineRunEntity(UUID.randomUUID(),pipeline,"MANUAL",null));
        UUID runId=run.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { executor.execute(()->execute(runId)); }
                catch(RuntimeException ex) { markFailed(runId,"Pipeline queue rejected run: "+ex.getMessage(),"[]"); }
            }
        });
        return run;
    }

    public PipelineRunEntity executeScheduled(UUID pipelineId,UUID scheduleId) {
        PipelineEntity pipeline=enabledPipeline(pipelineId);
        PipelineRunEntity run=runs.saveAndFlush(new PipelineRunEntity(UUID.randomUUID(),pipeline,"SCHEDULE",scheduleId));
        execute(run.getId());
        return find(run.getId());
    }

    @Transactional(readOnly=true)
    public PipelineRunEntity find(UUID id) {
        return runs.findById(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline run not found"));
    }
    @Transactional(readOnly=true)
    public List<PipelineRunEntity> history(UUID pipelineId) {
        if(!pipelines.existsById(pipelineId)) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline not found");
        return runs.findTop100ByPipelineIdOrderBySubmittedAtDesc(pipelineId);
    }

    private PipelineEntity enabledPipeline(UUID id) {
        PipelineEntity pipeline=pipelines.findById(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline not found"));
        if(!pipeline.isEnabled()) throw new ResponseStatusException(HttpStatus.CONFLICT,"Pipeline is disabled");
        return pipeline;
    }
    private void execute(UUID id) {
        markRunning(id);
        try {
            PipelineRunEntity run=find(id);
            var result=engine.execute(run.getDefinitionSnapshot());
            markSucceeded(id,result);
        } catch(Exception ex) {
            markFailed(id,ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage(),"[]");
        }
    }
    private void markRunning(UUID id) { runs.findById(id).ifPresent(run->{run.start();runs.save(run);}); }
    private void markSucceeded(UUID id,PipelineExecutionEngine.ExecutionResult result) {
        runs.findById(id).ifPresent(run->{run.succeed(result.recordsRead(),result.recordsWritten(),result.outputLocation(),
                result.stepResultsJson(),result.resultSummary());runs.save(run);});
    }
    private void markFailed(UUID id,String error,String steps) {
        runs.findById(id).ifPresent(run->{run.fail(error,steps);runs.save(run);});
    }
}
