package com.sbase.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineRunController {
    private final PipelineExecutionService execution;
    public PipelineRunController(PipelineExecutionService execution) { this.execution = execution; }

    @PostMapping("/{id}/runs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PipelineRunEntity submit(@PathVariable UUID id) { return execution.submit(id); }

    @GetMapping("/{id}/runs")
    public List<PipelineRunEntity> list(@PathVariable UUID id) { return execution.listRuns(id); }

    @GetMapping("/runs/{runId}")
    public PipelineRunEntity get(@PathVariable UUID runId) { return execution.getRun(runId); }
}
