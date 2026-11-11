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
    public PipelineRunEntity submit(@PathVariable UUID id, Authentication auth) { permissions.require(auth, "pipelines:write"); if (execution.pipelineWritesFeatures(id)) permissions.require(auth, "features:write"); return execution.submit(id, auth.getName()); }

    @GetMapping("/{id}/runs")
    public List<PipelineRunEntity> list(@PathVariable UUID id, Authentication auth) { permissions.require(auth, "pipelines:read"); boolean admin=auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin")) || permissions.has(auth.getName(), "platform:admin"); return execution.listRuns(id).stream().filter(run -> admin || auth.getName().equals(run.getSubmittedBy())).toList(); }

    @GetMapping("/runs/{runId}")
    public PipelineRunEntity get(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:read"); PipelineRunEntity run=execution.getRun(runId); requireOwner(run,auth); return run; }

    @PostMapping("/runs/{runId}/cancel")
    public PipelineRunEntity cancel(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:write"); requireOwner(execution.getRun(runId),auth); return execution.cancel(runId); }

    @GetMapping("/runs/{runId}/steps")
    public List<java.util.Map<String,Object>> steps(@PathVariable UUID runId, Authentication auth) { permissions.require(auth, "pipelines:read"); requireOwner(execution.getRun(runId),auth); return execution.stepRuns(runId); }

    private void requireOwner(PipelineRunEntity run, Authentication auth) {
        boolean admin=auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin")) || permissions.has(auth.getName(), "platform:admin");
        if (!admin && !auth.getName().equals(run.getSubmittedBy()))
            throw new org.springframework.security.access.AccessDeniedException("Pipeline run belongs to another subject");
    }
}
