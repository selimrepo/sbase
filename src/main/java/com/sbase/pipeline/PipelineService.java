package com.sbase.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.UUID;

@Service
public class PipelineService {
    private final PipelineRepository repository;
    private final PipelineDefinitionValidator validator;

    public PipelineService(PipelineRepository repository, PipelineDefinitionValidator validator) {
        this.repository = repository;
        this.validator = validator;
    }

    @Transactional
    public PipelineEntity create(PipelineRequest request) {
        validator.parseAndValidate(request.definitionJson());
        return repository.save(new PipelineEntity(request.name(), request.description(), request.definitionJson(), request.enabled()));
    }

    @Transactional(readOnly = true)
    public List<PipelineEntity> list() { return repository.findAllByOrderByUpdatedAtDesc(); }

    @Transactional(readOnly = true)
    public PipelineEntity get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found"));
    }

    @Transactional
    public PipelineEntity update(UUID id, PipelineRequest request) {
        validator.parseAndValidate(request.definitionJson());
        PipelineEntity pipeline = get(id);
        pipeline.update(request.name(), request.description(), request.definitionJson(), request.enabled());
        return pipeline;
    }

    @Transactional
    public void delete(UUID id) { repository.delete(get(id)); }
}
