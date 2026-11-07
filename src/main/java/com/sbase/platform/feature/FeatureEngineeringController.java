package com.sbase.platform.feature;
import jakarta.validation.Valid;import jakarta.validation.constraints.*;
import org.apache.spark.sql.Dataset;import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequestMapping("/api/platform/features")
public class FeatureEngineeringController {
 private final SparkFeatureEngineeringService service; private final com.sbase.platform.security.PermissionService permissions;
 public FeatureEngineeringController(SparkFeatureEngineeringService service,com.sbase.platform.security.PermissionService permissions){this.service=service;this.permissions=permissions;}
 public record MaterializeRequest(@NotBlank String inputUri,@NotBlank String outputUri,@NotEmpty List<@Valid Operation> operations){}
 public record Operation(@NotBlank String operation,@NotBlank String source,@NotBlank String output,int buckets){}
 @PostMapping("/materialize")
 public Map<String,Object> materialize(@Valid @RequestBody MaterializeRequest r,org.springframework.security.core.Authentication auth){
  permissions.require(auth,"features:write");
  var ops=r.operations().stream().map(o->new SparkFeatureEngineeringService.FeatureOperation(o.operation(),o.source(),o.output(),o.buckets())).toList();
  service.materialize(r.inputUri(),r.outputUri(),ops);
  return Map.of("status","SUCCEEDED","outputUri",r.outputUri(),"operationCount",ops.size());
 }
}
