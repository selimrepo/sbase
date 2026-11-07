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

    public PlatformMetadataController(@Qualifier("jdbcTemplate") JdbcTemplate db) {
        this.db = db;
    }

    public record Experiment(@NotBlank @Size(max=160) String name, String description, UUID datasetId,
                             @NotBlank String parametersJson) { }
    public record Model(@NotBlank @Size(max=160) String name, String description) { }
    public record ModelVersion(@NotBlank String artifactUri, @NotBlank String framework,
                               @NotBlank String parametersJson, @NotBlank String metricsJson, UUID experimentId) { }
    public record FeatureSet(@NotBlank @Size(max=160) String name, String description, UUID datasetId,
                             @NotBlank String entityKey, @NotBlank String schemaJson,
                             @NotBlank String transformationJson) { }
    public record Schedule(@NotBlank @Size(max=160) String name, UUID pipelineId,
                           @NotBlank @Size(max=120) String cronExpression) { }
    public record Dashboard(@NotBlank @Size(max=160) String name, String description, String layoutJson) { }

    @PostMapping("/experiments")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> experiment(@Valid @RequestBody Experiment r) {
        UUID id=UUID.randomUUID(); Instant now=Instant.now();
        db.update("INSERT INTO experiments(id,name,description,dataset_id,parameters_json,metrics_json,status,created_at,updated_at) VALUES(?,?,?,?,?,'{}','CREATED',?,?)",
                id,r.name(),r.description(),r.datasetId(),r.parametersJson(),now,now);
        return Map.of("id",id,"status","CREATED");
    }

    @GetMapping("/experiments")
    public List<Map<String,Object>> experiments() {
        return db.queryForList("SELECT * FROM experiments ORDER BY created_at DESC LIMIT 500");
    }

    @PostMapping("/models")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> model(@Valid @RequestBody Model r) {
        UUID id=UUID.randomUUID();
        db.update("INSERT INTO registered_models(id,name,description,created_at) VALUES(?,?,?,?)",id,r.name(),r.description(),Instant.now());
        return Map.of("id",id,"name",r.name());
    }

    @PostMapping("/models/{id}/versions")
    @ResponseStatus(HttpStatus.CREATED)
    @Transactional
    public Map<String,Object> version(@PathVariable UUID id,@Valid @RequestBody ModelVersion r) {
        db.queryForObject("SELECT id FROM registered_models WHERE id=? FOR UPDATE", UUID.class, id);
        Integer n=db.queryForObject("SELECT COALESCE(MAX(version),0)+1 FROM model_versions WHERE model_id=?",Integer.class,id);
        UUID v=UUID.randomUUID();
        db.update("INSERT INTO model_versions(id,model_id,version,experiment_id,artifact_uri,framework,parameters_json,metrics_json,stage,created_at) VALUES(?,?,?,?,?,?,?,?, 'NONE',?)",
                v,id,n,r.experimentId(),r.artifactUri(),r.framework(),r.parametersJson(),r.metricsJson(),Instant.now());
        return Map.of("id",v,"version",n);
    }

    @GetMapping("/models/{id}/versions")
    public List<Map<String,Object>> versions(@PathVariable UUID id) {
        return db.queryForList("SELECT * FROM model_versions WHERE model_id=? ORDER BY version DESC",id);
    }

    @PostMapping("/features")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> feature(@Valid @RequestBody FeatureSet r) {
        UUID id=UUID.randomUUID(); Instant now=Instant.now();
        db.update("INSERT INTO feature_sets(id,name,description,dataset_id,entity_key,feature_schema_json,transformation_json,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,1,?,?)",
                id,r.name(),r.description(),r.datasetId(),r.entityKey(),r.schemaJson(),r.transformationJson(),now,now);
        return Map.of("id",id,"version",1);
    }

    @GetMapping("/features")
    public List<Map<String,Object>> features() {
        return db.queryForList("SELECT * FROM feature_sets ORDER BY updated_at DESC LIMIT 500");
    }

    @PostMapping("/schedules")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> schedule(@Valid @RequestBody Schedule r) {
        var cron=org.springframework.scheduling.support.CronExpression.parse(r.cronExpression());
        var next=cron.next(java.time.LocalDateTime.now(java.time.ZoneOffset.UTC));
        if(next==null) throw new IllegalArgumentException("Cron expression has no next execution time");
        UUID id=UUID.randomUUID();
        db.update("INSERT INTO scheduled_jobs(id,name,pipeline_id,cron_expression,next_run_at,created_at) VALUES(?,?,?,?,?,?)",
                id,r.name(),r.pipelineId(),r.cronExpression(),next.toInstant(java.time.ZoneOffset.UTC),Instant.now());
        return Map.of("id",id,"enabled",true,"nextRunAt",next.toInstant(java.time.ZoneOffset.UTC));
    }

    @GetMapping("/schedules")
    public List<Map<String,Object>> schedules() {
        return db.queryForList("SELECT * FROM scheduled_jobs ORDER BY created_at DESC LIMIT 500");
    }

    @PostMapping("/dashboards")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String,Object> dashboard(@Valid @RequestBody Dashboard r, Authentication auth) {
        UUID id=UUID.randomUUID();
        db.update("INSERT INTO dashboards(id,name,description,layout_json,owner_subject,created_at) VALUES(?,?,?,?,?,?)",
                id,r.name(),r.description(),r.layoutJson()==null?"{}":r.layoutJson(),auth.getName(),Instant.now());
        return Map.of("id",id,"name",r.name());
    }

    @GetMapping("/dashboards")
    public List<Map<String,Object>> dashboards(Authentication auth) {
        return db.queryForList("SELECT * FROM dashboards WHERE owner_subject=? ORDER BY created_at DESC",auth.getName());
    }

    @GetMapping("/bi/summary")
    public Map<String,Object> summary() {
        Map<String,Object> result=new LinkedHashMap<>();
        for(String table:List.of("datasets","pipelines","experiments","registered_models","feature_sets","scheduled_jobs"))
            result.put(table,db.queryForObject("SELECT COUNT(*) FROM "+table,Long.class));
        return result;
    }
}
