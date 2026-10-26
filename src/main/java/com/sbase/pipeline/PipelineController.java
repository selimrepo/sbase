package com.sbase.pipeline;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {
    private final PipelineService service;
    public PipelineController(PipelineService service) { this.service = service; }

    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public PipelineEntity create(@Valid @RequestBody PipelineRequest request) { return service.create(request); }
    @GetMapping public List<PipelineEntity> list() { return service.list(); }
    @GetMapping("/{id}") public PipelineEntity get(@PathVariable UUID id) { return service.get(id); }
    @PutMapping("/{id}") public PipelineEntity update(@PathVariable UUID id, @Valid @RequestBody PipelineRequest request) { return service.update(id, request); }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) { service.delete(id); }
}