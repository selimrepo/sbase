package com.sbase.platform;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/platform")
public class PlatformMetadataController {
    private final JdbcTemplate db;
    private final com.sbase.platform.bi.PostgresBiAnalyticsService postgresAnalytics;
    private final com.sbase.platform.security.PermissionService permissions;
    private final com.sbase.platform.tenant.TenantContext tenants;
    @org.springframework.beans.factory.annotation.Autowired
    private com.sbase.platform.bi.PostgresTenantGovernanceAuditService postgresGovernanceAudit;

    public PlatformMetadataController(@Qualifier("jdbcTemplate") JdbcTemplate db,
            com.sbase.platform.bi.PostgresBiAnalyticsService postgresAnalytics,
            com.sbase.platform.security.PermissionService permissions,
            com.sbase.platform.tenant.TenantContext tenants) {
        this.db=db; this.postgresAnalytics=postgresAnalytics; this.permissions=permissions; this.tenants=tenants;
    }

    public record Experiment(@NotBlank @Size(max=160) String name,String description,UUID datasetId,
                             @NotBlank String parametersJson) {}
    public record Model(@NotBlank @Size(max=160) String name,String description) {}
    public record ModelVersion(@NotBlank String artifactUri,@NotBlank String framework,
                               @NotBlank String parametersJson,@NotBlank String metricsJson,UUID experimentId) {}
    public record FeatureSet(@NotBlank @Size(max=160) String name,String description,UUID datasetId,
                             @NotBlank String entityKey,@NotBlank String schemaJson,
                             @NotBlank String transformationJson) {}
    public record Schedule(@NotBlank @Size(max=160) String name,UUID pipelineId,
                           @NotBlank @Size(max=120) String cronExpression) {}
    public record Dashboard(@NotBlank @Size(max=160) String name,String description,String layoutJson) {}

    private UUID tenant(Authentication auth) { return tenants.tenantId(auth); }
    private void require(Authentication auth,String permission,boolean write) {
        permissions.require(auth,permission);
        if(write) tenants.requireWrite(auth); else tenant(auth);
    }
    private void requireDataset(UUID tenant,UUID dataset) {
        if(dataset!=null && !Boolean.TRUE.equals(db.query(
                "SELECT EXISTS(SELECT 1 FROM datasets WHERE id=? AND tenant_id=?)",
                rs->rs.next()&&rs.getBoolean(1),dataset,tenant)))
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Dataset not found");
    }

    @PostMapping("/experiments") @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> experiment(@Valid @RequestBody Experiment r,Authentication auth) {
        require(auth,"ml:write",true); UUID tenant=tenant(auth); requireDataset(tenant,r.datasetId());
        UUID id=UUID.randomUUID(); Instant now=Instant.now();
        db.update("INSERT INTO experiments(id,tenant_id,owner_subject,name,description,dataset_id,parameters_json,metrics_json,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'{}','CREATED',?,?)",
            id,tenant,auth.getName(),r.name(),r.description(),r.datasetId(),r.parametersJson(),now,now);
        return Map.of("id",id,"status","CREATED");
    }

    @GetMapping("/experiments")
    public List<Map<String,Object>> experiments(Authentication auth) {
        require(auth,"ml:read",false);
        return db.queryForList("SELECT * FROM experiments WHERE tenant_id=? ORDER BY created_at DESC LIMIT 500",tenant(auth));
    }

    @PostMapping("/models") @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> model(@Valid @RequestBody Model r,Authentication auth) {
        require(auth,"ml:write",true); UUID tenant=tenant(auth); UUID id=UUID.randomUUID();
        db.update("INSERT INTO registered_models(id,tenant_id,owner_subject,name,description,created_at) VALUES(?,?,?,?,?,?)",
            id,tenant,auth.getName(),r.name(),r.description(),Instant.now());
        return Map.of("id",id,"name",r.name());
    }

    @PostMapping("/models/{id}/versions") @ResponseStatus(HttpStatus.CREATED) @Transactional
    public Map<String,Object> version(@PathVariable UUID id,@Valid @RequestBody ModelVersion r,Authentication auth) {
        require(auth,"ml:write",true); UUID tenant=tenant(auth);
        db.queryForObject("SELECT id FROM registered_models WHERE id=? AND tenant_id=? FOR UPDATE",UUID.class,id,tenant);
        if(r.experimentId()!=null && !Boolean.TRUE.equals(db.query(
            "SELECT EXISTS(SELECT 1 FROM experiments WHERE id=? AND tenant_id=?)",
            rs->rs.next()&&rs.getBoolean(1),r.experimentId(),tenant)))
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Experiment not found");
        Integer n=db.queryForObject("SELECT COALESCE(MAX(version),0)+1 FROM model_versions WHERE model_id=? AND tenant_id=?",Integer.class,id,tenant);
        UUID v=UUID.randomUUID();
        db.update("INSERT INTO model_versions(id,tenant_id,model_id,version,experiment_id,artifact_uri,framework,parameters_json,metrics_json,stage,created_at) VALUES(?,?,?,?,?,?,?,?,?,'NONE',?)",
            v,tenant,id,n,r.experimentId(),r.artifactUri(),r.framework(),r.parametersJson(),r.metricsJson(),Instant.now());
        return Map.of("id",v,"version",n);
    }

    @GetMapping("/models/{id}/versions")
    public List<Map<String,Object>> versions(@PathVariable UUID id,Authentication auth) {
        require(auth,"ml:read",false);
        return db.queryForList("SELECT * FROM model_versions WHERE model_id=? AND tenant_id=? ORDER BY version DESC",id,tenant(auth));
    }

    @PostMapping("/features") @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> feature(@Valid @RequestBody FeatureSet r,Authentication auth) {
        require(auth,"features:write",true); UUID tenant=tenant(auth); requireDataset(tenant,r.datasetId());
        UUID id=UUID.randomUUID(); Instant now=Instant.now();
        db.update("INSERT INTO feature_sets(id,tenant_id,name,description,dataset_id,entity_key,feature_schema_json,transformation_json,version,created_at,updated_at,owner_subject) VALUES(?,?,?,?,?,?,?,?,1,?,?,?)",
            id,tenant,r.name(),r.description(),r.datasetId(),r.entityKey(),r.schemaJson(),r.transformationJson(),now,now,auth.getName());
        return Map.of("id",id,"version",1);
    }

    @GetMapping("/features")
    public List<Map<String,Object>> features(Authentication auth) {
        require(auth,"features:read",false); UUID tenant=tenant(auth);
        boolean admin=auth.getAuthorities().stream().anyMatch(x->x.getAuthority().equals("ROLE_ADMIN")||x.getAuthority().equals("PERM_platform:admin"))
            ||permissions.has(auth.getName(),"platform:admin");
        return db.queryForList("""
            SELECT f.* FROM feature_sets f
             WHERE f.tenant_id=? AND (?=TRUE OR f.owner_subject=?
                OR EXISTS(SELECT 1 FROM feature_access_grants g WHERE g.feature_set_id=f.id AND g.tenant_id=f.tenant_id AND g.subject=? AND g.permission IN ('READ','WRITE','ADMIN'))
                OR (f.owner_subject IS NULL AND NOT EXISTS(SELECT 1 FROM feature_access_grants g WHERE g.feature_set_id=f.id AND g.tenant_id=f.tenant_id)))
             ORDER BY f.updated_at DESC LIMIT 500
            """,tenant,admin,auth.getName(),auth.getName());
    }

    @PostMapping("/schedules") @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> schedule(@Valid @RequestBody Schedule r,Authentication auth) {
        require(auth,"pipelines:write",true); UUID tenant=tenant(auth);
        if(r.pipelineId()!=null && !Boolean.TRUE.equals(db.query(
            "SELECT EXISTS(SELECT 1 FROM pipelines WHERE id=? AND tenant_id=?)",
            rs->rs.next()&&rs.getBoolean(1),r.pipelineId(),tenant)))
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline not found");
        var cron=org.springframework.scheduling.support.CronExpression.parse(r.cronExpression());
        var next=cron.next(java.time.LocalDateTime.now(java.time.ZoneOffset.UTC));
        if(next==null)throw new IllegalArgumentException("Cron expression has no next execution time");
        UUID id=UUID.randomUUID();
        db.update("INSERT INTO scheduled_jobs(id,tenant_id,owner_subject,name,pipeline_id,cron_expression,next_run_at,created_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)",
            id,tenant,auth.getName(),r.name(),r.pipelineId(),r.cronExpression(),next.toInstant(java.time.ZoneOffset.UTC),Instant.now(),auth.getName());
        return Map.of("id",id,"enabled",true,"nextRunAt",next.toInstant(java.time.ZoneOffset.UTC));
    }

    @GetMapping("/schedules")
    public List<Map<String,Object>> schedules(Authentication auth) {
        require(auth,"pipelines:read",false); UUID tenant=tenant(auth);
        return db.queryForList("SELECT * FROM scheduled_jobs WHERE tenant_id=? AND (owner_subject=? OR created_by=?) ORDER BY created_at DESC LIMIT 500",
            tenant,auth.getName(),auth.getName());
    }

    @PostMapping("/dashboards") @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> dashboard(@Valid @RequestBody Dashboard r,Authentication auth) {
        require(auth,"bi:write",true); UUID tenant=tenant(auth); UUID id=UUID.randomUUID();
        db.update("INSERT INTO dashboards(id,tenant_id,name,description,layout_json,owner_subject,created_at) VALUES(?,?,?,?,?,?,?)",
            id,tenant,r.name(),r.description(),r.layoutJson()==null?"{}":r.layoutJson(),auth.getName(),Instant.now());
        return Map.of("id",id,"name",r.name());
    }

    @GetMapping("/dashboards")
    public List<Map<String,Object>> dashboards(Authentication auth) {
        require(auth,"bi:read",false);
        return db.queryForList("SELECT * FROM dashboards WHERE tenant_id=? AND owner_subject=? ORDER BY created_at DESC",tenant(auth),auth.getName());
    }

    @PostMapping("/bi/tenant-governance/audit")
    public Map<String,Object> auditDorisTenantGovernance(Authentication auth) {
        require(auth,"platform:admin",false);
        return postgresGovernanceAudit.auditAllDatabases(auth.getName());
    }

    @GetMapping("/bi/summary")
    public Map<String,Object> summary(Authentication auth) {
        require(auth,"platform:admin",false);
        UUID tenant=tenant(auth);
        Map<String,Object> metadata=new LinkedHashMap<>();
        for(String table:List.of("datasets","pipelines","experiments","registered_models","feature_sets","scheduled_jobs"))
            metadata.put(table,db.queryForObject("SELECT COUNT(*) FROM "+table+" WHERE tenant_id=?",Long.class,tenant));
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("metadata",metadata);
        // PostgreSQL table inventory and audit activity are cluster-wide operational data;
        // only tenant platform administrators may inspect this legacy endpoint.
        result.put("analytics",postgresAnalytics.summary());
        return result;
    }
}
