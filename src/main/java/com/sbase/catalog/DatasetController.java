package com.sbase.catalog;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetService service;
    public DatasetController(DatasetService service) { this.service=service; }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public DatasetEntity create(@Valid @RequestBody DatasetRequest request,Authentication auth) { return service.create(request,auth); }
    @GetMapping public List<DatasetEntity> list(Authentication auth) { return service.list(auth); }
    @GetMapping("/{id}") public DatasetEntity get(@PathVariable UUID id,Authentication auth) { return service.get(id,auth); }
    @PutMapping("/{id}") public DatasetEntity update(@PathVariable UUID id,@Valid @RequestBody DatasetRequest request,Authentication auth) { return service.update(id,request,auth); }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) public void delete(@PathVariable UUID id,Authentication auth) { service.delete(id,auth); }
}
