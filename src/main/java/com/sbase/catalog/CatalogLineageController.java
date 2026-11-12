package com.sbase.catalog;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.validation.annotation.Validated;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import com.sbase.platform.security.PermissionService;
import java.util.List;
import java.util.UUID;

@RestController
@Validated
@RequestMapping("/api/catalog")
public class CatalogLineageController {
    private final CatalogLineageService service;
    private final PermissionService permissions;
    public CatalogLineageController(CatalogLineageService service,PermissionService permissions) {
        this.service=service; this.permissions=permissions;
    }

    public record CreateAssetRequest(
        @NotBlank @Pattern(regexp="DATA_SOURCE|CONNECTOR|TABLE|PIPELINE|PIPELINE_STEP|FEATURE_SET|MODEL|REPORT|REPORT_FIELD|DASHBOARD|WIDGET")
        String assetType,
        @NotBlank @Size(max=512) String externalKey,
        @NotBlank @Size(max=240) String displayName,
        @Size(max=160) String namespace,@Size(max=512) String physicalName,@Size(max=10000) String description,
        @Size(max=160) String ownerTeam,@Size(max=120) String domain,String classification,
        @Size(max=50) List<@Size(max=80) String> tags,java.util.Map<String,Object> metadata,UUID parentId) {}
    public record CreateColumnRequest(
        @NotBlank @Pattern(regexp="[A-Za-z_][A-Za-z0-9_$]{0,254}") String name,
        @NotBlank @Size(max=160) String dataType,Boolean nullable,@Min(1) @Max(100000) Integer ordinalPosition,
        @Size(max=10000) String description,@Size(max=160) String ownerTeam,String classification,
        @Size(max=50) List<@Size(max=80) String> tags,java.util.Map<String,Object> metadata) {}
    public record GovernanceRequest(@Size(max=10000) String description,@Size(max=200) String ownerSubject,
        @Size(max=160) String ownerTeam,@Size(max=120) String domain,String classification,
        @Size(max=50) List<@Size(max=80) String> tags,java.util.Map<String,Object> metadata,String lifecycleState) {}
    public record CreateEdgeRequest(@NotNull UUID fromAssetId,@NotNull UUID toAssetId,@NotBlank String edgeType,
        @NotBlank String evidenceType,@Size(max=512) String evidenceRef,@Size(max=20000) String expressionText,
        @DecimalMin("0.0") @DecimalMax("1.0") java.math.BigDecimal confidence,@Min(1) Long effectiveVersion,UUID runId) {}

    @PostMapping("/assets") @ResponseStatus(HttpStatus.CREATED)
    public CatalogLineageService.AssetView create(@Valid @RequestBody CreateAssetRequest r,Authentication auth) {
        permissions.require(auth,"catalog:write");
        return service.createAsset(new CatalogLineageService.CreateAsset(r.assetType(),r.externalKey(),r.displayName(),
            r.namespace(),r.physicalName(),r.description(),r.ownerTeam(),r.domain(),r.classification(),r.tags(),r.metadata(),r.parentId()),auth);
    }
    @GetMapping("/assets")
    public List<CatalogLineageService.AssetView> list(@RequestParam(required=false) String type,
            @RequestParam(defaultValue="100") @Min(1) @Max(200) Integer limit,Authentication auth) {
        permissions.require(auth,"catalog:read"); return service.list(type,limit,auth);
    }
    @GetMapping("/assets/{id}")
    public CatalogLineageService.AssetView get(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"catalog:read"); return service.get(id,auth);
    }
    @PatchMapping("/assets/{id}/governance")
    public CatalogLineageService.AssetView governance(@PathVariable UUID id,@Valid @RequestBody GovernanceRequest r,Authentication auth) {
        permissions.require(auth,"catalog:write");
        return service.updateGovernance(id,new CatalogLineageService.GovernanceUpdate(r.description(),r.ownerSubject(),
            r.ownerTeam(),r.domain(),r.classification(),r.tags(),r.metadata(),r.lifecycleState()),auth);
    }
    @GetMapping("/assets/{id}/schema-versions")
    public List<CatalogLineageService.SchemaVersionView> schemaHistory(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"catalog:read"); return service.schemaHistory(id,auth);
    }
    @GetMapping("/assets/{id}/columns")
    public List<CatalogLineageService.AssetView> columns(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"catalog:read"); return service.columns(id,auth);
    }
    @PostMapping("/assets/{id}/columns") @ResponseStatus(HttpStatus.CREATED)
    public CatalogLineageService.AssetView addColumn(@PathVariable UUID id,@Valid @RequestBody CreateColumnRequest r,Authentication auth) {
        permissions.require(auth,"catalog:write");
        return service.registerColumn(id,new CatalogLineageService.CreateColumn(r.name(),r.dataType(),r.nullable(),
            r.ordinalPosition(),r.description(),r.ownerTeam(),r.classification(),r.tags(),r.metadata()),auth);
    }
    @PostMapping("/lineage/edges") @ResponseStatus(HttpStatus.CREATED)
    public CatalogLineageService.EdgeView connect(@Valid @RequestBody CreateEdgeRequest r,Authentication auth) {
        permissions.require(auth,"lineage:write");
        return service.connect(new CatalogLineageService.CreateEdge(r.fromAssetId(),r.toAssetId(),r.edgeType(),
            r.evidenceType(),r.evidenceRef(),r.expressionText(),r.confidence(),r.effectiveVersion(),r.runId()),auth);
    }
    @GetMapping("/lineage/graph")
    public CatalogLineageService.GraphView graph(@RequestParam UUID rootId,
            @RequestParam(defaultValue="BOTH") String direction,@RequestParam(defaultValue="3") @Min(1) @Max(8) Integer depth,
            Authentication auth) {
        permissions.require(auth,"lineage:read"); return service.graph(rootId,direction,depth,auth);
    }
    @GetMapping("/lineage/impact")
    public CatalogLineageService.GraphView impact(@RequestParam UUID assetId,
            @RequestParam(defaultValue="6") @Min(1) @Max(8) Integer depth,Authentication auth) {
        permissions.require(auth,"lineage:read"); return service.impact(assetId,depth,auth);
    }
}
