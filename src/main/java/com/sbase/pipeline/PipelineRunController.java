package com.sbase.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineRunController {
    private final PipelineRunService service;
    public PipelineRunController(PipelineRunService service){this.service=service;}

    @PostMapping("/{id}/runs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PipelineRunEntity submit(@PathVariable UUID id){return service.submit(id);}

    @GetMapping("/{id}/runs")
    public List<PipelineRunEntity> history(@PathVariable UUID id){return service.history(id);}

    @GetMapping("/runs/{runId}")
    public PipelineRunEntity get(@PathVariable UUID runId){return service.find(runId);}
}
