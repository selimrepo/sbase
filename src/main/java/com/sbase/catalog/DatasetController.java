package com.sbase.catalog;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetService service;
    private final PermissionService permissions;
    public DatasetController(DatasetService service,PermissionService permissions) { this.service=service; this.permissions=permissions; }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public DatasetEntity create(@Valid @RequestBody DatasetRequest request,Authentication auth) {
        permissions.require(auth,"datasets:write"); return service.create(request,auth);
    }
    @GetMapping public List<DatasetEntity> list(Authentication auth) {
        permissions.require(auth,"datasets:read"); return service.list(auth);
    }
    @GetMapping("/{id}") public DatasetEntity get(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"datasets:read"); return service.get(id,auth);
    }
    @PutMapping("/{id}") public DatasetEntity update(@PathVariable UUID id,@Valid @RequestBody DatasetRequest request,Authentication auth) {
        permissions.require(auth,"datasets:write"); return service.update(id,request,auth);
    }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) public void delete(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"datasets:write"); service.delete(id,auth);
    }
}
