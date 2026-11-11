package com.sbase.pipeline;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final PipelineService service;
    private final PermissionService permissions;
    public PipelineController(PipelineService service,PermissionService permissions) { this.service=service; this.permissions=permissions; }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public PipelineEntity create(@Valid @RequestBody PipelineRequest request,Authentication auth) {
        permissions.require(auth,"pipelines:write"); return service.create(request,auth);
    }
    @GetMapping public List<PipelineEntity> list(Authentication auth) {
        permissions.require(auth,"pipelines:read"); return service.list(auth);
    }
    @GetMapping("/{id}") public PipelineEntity get(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"pipelines:read"); return service.get(id,auth);
    }
    @PutMapping("/{id}") public PipelineEntity update(@PathVariable UUID id,@Valid @RequestBody PipelineRequest request,Authentication auth) {
        permissions.require(auth,"pipelines:write"); return service.update(id,request,auth);
    }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) public void delete(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"pipelines:write"); service.delete(id,auth);
    }
}
