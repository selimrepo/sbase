package com.sbase.pipeline;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.Authentication;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final PipelineService service;
    public PipelineController(PipelineService service) { this.service=service; }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public PipelineEntity create(@Valid @RequestBody PipelineRequest request,Authentication auth) { return service.create(request,auth); }
    @GetMapping public List<PipelineEntity> list(Authentication auth) { return service.list(auth); }
    @GetMapping("/{id}") public PipelineEntity get(@PathVariable UUID id,Authentication auth) { return service.get(id,auth); }
    @PutMapping("/{id}") public PipelineEntity update(@PathVariable UUID id,@Valid @RequestBody PipelineRequest request,Authentication auth) { return service.update(id,request,auth); }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) public void delete(@PathVariable UUID id,Authentication auth) { service.delete(id,auth); }
}
