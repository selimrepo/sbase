package com.sbase.platform.ml;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/ml")
public class MlLifecycleController {
 private final MlLifecycleService ml; private final com.sbase.platform.tenant.TenantContext tenants;
 private final com.sbase.platform.security.PermissionService permissions;
 public MlLifecycleController(MlLifecycleService ml,com.sbase.platform.tenant.TenantContext tenants,com.sbase.platform.security.PermissionService permissions){this.ml=ml;this.tenants=tenants;this.permissions=permissions;}
 public record ExperimentRequest(@NotBlank @Size(max=160) String name,String description){}
 public record TrainRequest(@NotNull UUID experimentId,@NotBlank @Size(max=160) String runName,
  @NotBlank String algorithm,@NotBlank String taskType,@NotBlank String datasetTable,
  @NotEmpty @Size(max=100) List<@Pattern(regexp="[A-Za-z_][A-Za-z0-9_]{0,127}") String> featureColumns,
  @NotBlank @Pattern(regexp="[A-Za-z_][A-Za-z0-9_]{0,127}") String labelColumn,
  @Min(1) @Max(10) int crossValidationFolds,JsonNode parameters,JsonNode searchSpace){}
 public record RegisterRequest(@NotNull UUID runId,@NotBlank @Size(max=160) String name,String description){}
 public record DeployRequest(@NotNull UUID modelVersionId,@NotBlank @Size(max=160) String deploymentName){}
 public record PredictionRequest(@NotEmpty Map<String,Object> features){}
 public record BatchRequest(@NotEmpty @Size(max=10000) List<Map<String,Object>> records){}
 private UUID tenant(Authentication a){return tenants.tenantId(a);}
 private void require(Authentication a,String p){permissions.require(a,p);if(Set.of("ml:write","ml:deploy","bi:write").contains(p))tenants.requireWrite(a);}
 @PostMapping("/experiments") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> createExperiment(@Valid @RequestBody ExperimentRequest r,Authentication a){
  require(a,"ml:write");return Map.of("id",ml.createExperiment(tenant(a),r.name(),r.description(),a.getName()));
 }
 @GetMapping("/experiments") public List<Map<String,Object>> experiments(Authentication a){require(a,"ml:read");return ml.experiments(tenant(a),a.getName());}
 @PostMapping("/runs") @ResponseStatus(HttpStatus.ACCEPTED)
 public Map<String,Object> train(@Valid @RequestBody TrainRequest r,Authentication a){
  require(a,"ml:write");UUID id=ml.submit(tenant(a),r.experimentId(),r.runName(),r.algorithm().toUpperCase(Locale.ROOT),r.taskType().toUpperCase(Locale.ROOT),r.datasetTable(),r.featureColumns(),r.labelColumn(),r.crossValidationFolds(),r.parameters(),r.searchSpace(),a.getName());return Map.of("runId",id,"status","QUEUED");
 }
 @GetMapping("/experiments/{id}/runs") public List<Map<String,Object>> compare(@PathVariable UUID id,Authentication a){require(a,"ml:read");return ml.compare(tenant(a),id);}
 @PostMapping("/models") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> register(@Valid @RequestBody RegisterRequest r,Authentication a){require(a,"ml:write");return Map.of("modelId",ml.registerModel(tenant(a),r.runId(),r.name(),r.description(),a.getName()));}
 @GetMapping("/models") public List<Map<String,Object>> models(Authentication a){require(a,"ml:read");return ml.models(tenant(a));}
 @PostMapping("/deployments") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> deploy(@Valid @RequestBody DeployRequest r,Authentication a){require(a,"ml:deploy");return Map.of("deploymentId",ml.deploy(tenant(a),r.modelVersionId(),r.deploymentName(),a.getName()),"status","ACTIVE");}
 @PostMapping("/deployments/{id}/predict")
 public Map<String,Object> predict(@PathVariable UUID id,@Valid @RequestBody PredictionRequest r,Authentication a){require(a,"ml:predict");return ml.predict(tenant(a),id,r.features());}
 @PostMapping("/deployments/{id}/batch")
 public Map<String,Object> batch(@PathVariable UUID id,@Valid @RequestBody BatchRequest r,Authentication a){require(a,"ml:predict");return Map.of("batchId",ml.batchPredict(tenant(a),id,r.records()),"status","QUEUED","recordCount",r.records().size());}
 @PutMapping("/model-versions/{id}/stage") @ResponseStatus(HttpStatus.NO_CONTENT)
 public void promote(@PathVariable UUID id,@RequestParam String stage,Authentication a){require(a,"ml:deploy");ml.promote(tenant(a),id,stage.toUpperCase(Locale.ROOT));}
 @GetMapping("/model-versions/{id}/lineage") public Map<String,Object> lineage(@PathVariable UUID id,Authentication a){require(a,"ml:read");return ml.lineage(tenant(a),id);}
 @GetMapping("/batches/{id}") public Map<String,Object> batch(@PathVariable UUID id,Authentication a){require(a,"ml:read");return ml.batchResult(tenant(a),id);}
 @GetMapping("/deployments/{id}/monitoring") public Map<String,Object> monitor(@PathVariable UUID id,Authentication a){require(a,"ml:read");return ml.monitor(tenant(a),id);}
}
