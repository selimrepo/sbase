package com.sbase.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.UUID;

@Service
public class PipelineService {
    private final PipelineRepository repository;
    private final ObjectMapper mapper;
    public PipelineService(PipelineRepository repository, ObjectMapper mapper) { this.repository = repository; this.mapper = mapper; }

    @Transactional
    public PipelineEntity create(PipelineRequest request) {
        validateDefinition(request.definitionJson());
        return repository.save(new PipelineEntity(request.name(), request.description(), request.definitionJson()));
    }
    @Transactional(readOnly = true)
    public List<PipelineEntity> list() { return repository.findAllByOrderByUpdatedAtDesc(); }
    @Transactional(readOnly = true)
    public PipelineEntity get(UUID id) { return repository.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found")); }
    @Transactional
    public PipelineEntity update(UUID id, PipelineRequest request) {
        validateDefinition(request.definitionJson());
        PipelineEntity pipeline = get(id);
        pipeline.update(request.name(), request.description(), request.definitionJson(), request.enabled());
        return pipeline;
    }
    @Transactional
    public void delete(UUID id) { repository.delete(get(id)); }

    private void validateDefinition(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            if (root == null || !root.isObject() || !root.has("steps") || !root.get("steps").isArray())
                throw new IllegalArgumentException("Pipeline definition must be an object containing a steps array");
            if (root.get("steps").isEmpty()) throw new IllegalArgumentException("Pipeline must contain at least one step");
            for (JsonNode step : root.get("steps")) {
                if (!step.isObject() || !step.hasNonNull("type") || !step.get("type").isTextual())
                    throw new IllegalArgumentException("Each pipeline step must define a string type");
            }
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid pipeline definition: " + ex.getMessage());
        }
    }
}