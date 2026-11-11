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
    private final PipelineService pipelines;
    private final PermissionService permissions;
    public PipelineRunController(PipelineExecutionService execution,PipelineService pipelines,PermissionService permissions) {
        this.execution=execution; this.pipelines=pipelines; this.permissions=permissions;
    }
    @PostMapping("/{id}/runs") @ResponseStatus(HttpStatus.ACCEPTED)
    public PipelineRunEntity submit(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"pipelines:write");
        pipelines.get(id,auth);
        if(execution.pipelineWritesFeatures(id)) permissions.require(auth,"features:write");
        return execution.submit(id,auth.getName());
    }
    @GetMapping("/{id}/runs")
    public List<PipelineRunEntity> list(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"pipelines:read"); pipelines.get(id,auth);
        return execution.listRuns(id).stream().filter(run -> auth.getName().equals(run.getSubmittedBy())
            || auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin"))).toList();
    }
    @GetMapping("/runs/{runId}")
    public PipelineRunEntity get(@PathVariable UUID runId,Authentication auth) {
        permissions.require(auth,"pipelines:read"); PipelineRunEntity run=execution.getRun(runId);
        pipelines.get(run.getPipelineId(),auth); requireOwner(run,auth); return run;
    }
    @PostMapping("/runs/{runId}/cancel")
    public PipelineRunEntity cancel(@PathVariable UUID runId,Authentication auth) {
        permissions.require(auth,"pipelines:write"); PipelineRunEntity run=execution.getRun(runId);
        pipelines.get(run.getPipelineId(),auth); requireOwner(run,auth); return execution.cancel(runId);
    }
    @GetMapping("/runs/{runId}/steps")
    public List<java.util.Map<String,Object>> steps(@PathVariable UUID runId,Authentication auth) {
        permissions.require(auth,"pipelines:read"); PipelineRunEntity run=execution.getRun(runId);
        pipelines.get(run.getPipelineId(),auth); requireOwner(run,auth); return execution.stepRuns(runId);
    }
    private void requireOwner(PipelineRunEntity run,Authentication auth) {
        boolean admin=auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin"))
            || permissions.has(auth.getName(),"platform:admin");
        if(!admin && !auth.getName().equals(run.getSubmittedBy()))
            throw new org.springframework.security.access.AccessDeniedException("Pipeline run belongs to another subject");
    }
}
