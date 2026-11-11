package com.sbase.platform.feature;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/feature-store")
public class PointInTimeFeatureController {
 private final PointInTimeFeatureService history;private final FeatureAccessService access;private final com.sbase.platform.tenant.TenantContext tenants;
 public PointInTimeFeatureController(PointInTimeFeatureService history,FeatureAccessService access,com.sbase.platform.tenant.TenantContext tenants){this.history=history;this.access=access;this.tenants=tenants;}
 public record MaterializeRequest(@Positive int featureVersion,Instant eventTime,@NotEmpty Map<String,Object> values,String sourceRunId){}
 @PutMapping("/{featureSet}/entities/{entityKey}/versions") @ResponseStatus(HttpStatus.NO_CONTENT)
 public void materialize(@PathVariable String featureSet,@PathVariable String entityKey,@Valid @RequestBody MaterializeRequest r,Authentication a){
  access.require(a,featureSet,true);history.materialize(tenants.tenantId(a),featureSet,entityKey,r.featureVersion(),r.eventTime(),r.values(),r.sourceRunId());
 }
 @GetMapping("/{featureSet}/entities/{entityKey}/as-of")
 public Map<String,Object> asOf(@PathVariable String featureSet,@PathVariable String entityKey,@RequestParam Instant timestamp,Authentication a){
  access.require(a,featureSet,false);return history.asOf(tenants.tenantId(a),featureSet,entityKey,timestamp);
 }
 @PostMapping("/{featureSet}/entities/{entityKey}/parity")
 public Map<String,Object> parity(@PathVariable String featureSet,@PathVariable String entityKey,@RequestParam Instant asOf,Authentication a){
  access.require(a,featureSet,false);return history.parity(tenants.tenantId(a),featureSet,entityKey,asOf);
 }
}
