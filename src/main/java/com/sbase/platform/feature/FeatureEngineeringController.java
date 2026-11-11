package com.sbase.platform.feature;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/platform/features")
public class FeatureEngineeringController {
    private final SparkFeatureEngineeringService service;
    private final FeatureAccessService access;
    private final JdbcTemplate db;
    private final ObjectMapper mapper;
    private final com.sbase.platform.tenant.TenantContext tenants;

    public FeatureEngineeringController(SparkFeatureEngineeringService service,
                                        FeatureAccessService access,
                                        @Qualifier("jdbcTemplate") JdbcTemplate db,
                                        ObjectMapper mapper,
                                        com.sbase.platform.tenant.TenantContext tenants) {
        this.service = service;
        this.access = access;
        this.db = db;
        this.mapper = mapper;
        this.tenants = tenants;
    }

    public record MaterializeRequest(@NotBlank String featureSetName, @NotBlank String inputUri,
                                     @NotBlank String outputUri, @NotEmpty List<@Valid Operation> operations) {}
    public record Operation(@NotBlank String operation, @NotBlank String source, @NotBlank String output, int buckets) {}
    private record FeatureSetRef(UUID id, int version) {}

    @PostMapping("/materialize")
    public Map<String,Object> materialize(@Valid @RequestBody MaterializeRequest request, Authentication auth) {
        access.require(auth, request.featureSetName(), true);
        FeatureSetRef feature = db.query("""
            SELECT id,version FROM feature_sets WHERE name=? AND tenant_id=?
            """, rs -> rs.next() ? new FeatureSetRef(rs.getObject("id", UUID.class), rs.getInt("version")) : null,
            request.featureSetName(), tenants.tenantId(auth));
        if (feature == null) throw new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.NOT_FOUND, "Feature set not found");

        UUID materializationId = UUID.randomUUID();
        List<SparkFeatureEngineeringService.FeatureOperation> operations = request.operations().stream()
            .map(op -> new SparkFeatureEngineeringService.FeatureOperation(op.operation(), op.source(), op.output(), op.buckets()))
            .toList();
        String transformation;
        try { transformation = mapper.writeValueAsString(request.operations()); }
        catch (Exception ex) { throw new IllegalArgumentException("Could not serialize feature transformation", ex); }

        db.update("""
            INSERT INTO feature_materializations(id,feature_set_id,version,output_uri,status,started_at)
            VALUES(?,?,?,?,'RUNNING',now())
            """, materializationId, feature.id(), feature.version(), request.outputUri());
        try {
            service.materialize(request.inputUri(), request.outputUri(), operations);
            db.update("UPDATE feature_materializations SET status='SUCCEEDED',finished_at=now() WHERE id=?", materializationId);
            db.update("""
                INSERT INTO feature_lineage(id,feature_set_id,source_dataset_id,source_columns_json,
                                            parent_feature_ids_json,transformation_json,created_by)
                VALUES(?,?,NULL,?,'[]',?,?)
                """, UUID.randomUUID(), feature.id(),
                mapper.writeValueAsString(request.operations().stream().map(Operation::source).distinct().toList()),
                transformation, auth.getName());
            return Map.of("id", materializationId, "status", "SUCCEEDED", "featureSet", request.featureSetName(),
                "version", feature.version(), "outputUri", request.outputUri(), "operationCount", operations.size());
        } catch (Exception ex) {
            String message = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            db.update("UPDATE feature_materializations SET status='FAILED',finished_at=now(),error_message=? WHERE id=?",
                message, materializationId);
            throw ex instanceof RuntimeException runtime ? runtime : new IllegalStateException(message, ex);
        }
    }
}
