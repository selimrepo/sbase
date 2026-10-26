package com.sbase.catalog;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetService service;
    public DatasetController(DatasetService service) { this.service = service; }

    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public DatasetEntity create(@Valid @RequestBody DatasetRequest request) { return service.create(request); }
    @GetMapping public List<DatasetEntity> list() { return service.list(); }
    @GetMapping("/{id}") public DatasetEntity get(@PathVariable UUID id) { return service.get(id); }
    @PutMapping("/{id}") public DatasetEntity update(@PathVariable UUID id, @Valid @RequestBody DatasetRequest request) { return service.update(id, request); }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) { service.delete(id); }
}