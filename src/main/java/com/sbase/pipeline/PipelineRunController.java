package com.sbase.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineRunController {
    private final PipelineExecutionService execution;
    private final PermissionService permissions;
    public PipelineRunController(PipelineExecutionService execution, PermissionService permissions) { this.execution = execution; this.permissions = permissions; }

    @PostMapping("/{id}/runs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PipelineRunEntity submit(@PathVariable UUID id, Authentication auth) { permissions.require(auth, "pipelines:write"); return execution.submit(id); }

    @GetMapping("/{id}/runs")
    public List<PipelineRunEntity> list(@PathVariable UUID id, Authentication auth) { permissions.require(auth, "pipelines:read"); return execution.listRuns(id); }

    @GetMapping("/runs/{runId}")
    public PipelineRunEntity get(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:read"); return execution.getRun(runId); }

    @PostMapping("/runs/{runId}/cancel")
    public PipelineRunEntity cancel(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:write"); return execution.cancel(runId); }

    @GetMapping("/runs/{runId}/steps")
    public List<java.util.Map<String,Object>> steps(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:read"); return execution.stepRuns(runId); }
}
