package com.sbase.catalog;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.UUID;

@Service
public class DatasetService {
    private final DatasetRepository repository;
    public DatasetService(DatasetRepository repository) { this.repository = repository; }

    @Transactional
    public DatasetEntity create(DatasetRequest request) {
        if (repository.findByNamespaceAndTableName(request.namespace(), request.tableName()).isPresent())
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Dataset table is already registered");
        return repository.save(new DatasetEntity(request.name(), request.namespace(), request.tableName(),
                request.description(), request.sourceType(), normalizeSchema(request.schemaJson())));
    }
    @Transactional(readOnly = true)
    public List<DatasetEntity> list() { return repository.findAllByOrderByUpdatedAtDesc(); }
    @Transactional(readOnly = true)
    public DatasetEntity get(UUID id) { return repository.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Dataset not found")); }
    @Transactional
    public DatasetEntity update(UUID id, DatasetRequest request) {
        DatasetEntity dataset = get(id);
        repository.findByNamespaceAndTableName(request.namespace(), request.tableName())
                .filter(other -> !other.getId().equals(id))
                .ifPresent(other -> { throw new ResponseStatusException(HttpStatus.CONFLICT, "Dataset table is already registered"); });
        if (!dataset.getNamespace().equals(request.namespace()) || !dataset.getTableName().equals(request.tableName()))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Dataset namespace and table name are immutable");
        dataset.update(request.name(), request.description(), normalizeSchema(request.schemaJson()));
        return dataset;
    }
    @Transactional
    public void delete(UUID id) { repository.delete(get(id)); }
    private String normalizeSchema(String schema) { return schema == null || schema.isBlank() ? "[]" : schema; }
}