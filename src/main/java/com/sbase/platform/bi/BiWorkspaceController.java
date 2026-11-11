package com.sbase.platform.bi;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/bi")
public class BiWorkspaceController {
 private final BiWorkspaceService bi;private final com.sbase.platform.tenant.TenantContext tenants;private final com.sbase.platform.security.PermissionService permissions;
 public BiWorkspaceController(BiWorkspaceService bi,com.sbase.platform.tenant.TenantContext tenants,com.sbase.platform.security.PermissionService permissions){this.bi=bi;this.tenants=tenants;this.permissions=permissions;}
 public record Metric(@NotBlank String column,@NotBlank String aggregate,@NotBlank @Pattern(regexp="[A-Za-z_][A-Za-z0-9_]{0,127}") String alias){}
 public record Filter(@NotBlank String column,@NotBlank String operator,Object value){}
 public record Sort(@NotBlank String column,String direction){}
 public record QueryRequest(@NotBlank String table,List<@Pattern(regexp="[A-Za-z_][A-Za-z0-9_]{0,127}") String> dimensions,
  @NotEmpty @Size(max=20) List<@Valid Metric> metrics,List<@Valid Filter> filters,List<@Valid Sort> orderBy,@Min(1) @Max(10000) Integer limit){}
 public record ReportRequest(@NotBlank @Size(max=160) String name,String description,@NotNull @Valid QueryRequest definition){}
 public record ShareRequest(@NotBlank String subject,@NotBlank String permission){}
 public record DashboardRequest(@NotBlank @Size(max=160) String name,String description,Map<String,Object> layout,boolean shared){}
 public record WidgetRequest(UUID reportId,@NotBlank @Size(max=160) String title,@NotBlank String type,Map<String,Object> position,Map<String,Object> options){}
 public record ScheduleRequest(@NotNull UUID reportId,@NotBlank String cronExpression,@NotBlank String timezone,@NotBlank String outputFormat,String deliveryTarget){}
 public record DrillDownRequest(@NotNull @Valid QueryRequest definition,@NotBlank String dimension,Object value,List<String> nextDimensions){}
 public record DrillThroughRequest(@NotBlank String table,@NotEmpty List<String> columns,List<@Valid Filter> filters,@Min(1) @Max(1000) Integer limit){}
 private UUID tenant(Authentication a){return tenants.tenantId(a);}
 private void require(Authentication a,String p){permissions.require(a,p);}
 private GovernedBiQueryService.QueryDefinition definition(QueryRequest r){
  return new GovernedBiQueryService.QueryDefinition(r.table(),r.dimensions()==null?List.of():r.dimensions(),
   r.metrics().stream().map(m->new GovernedBiQueryService.Metric(m.column(),m.aggregate(),m.alias())).toList(),
   r.filters()==null?List.of():r.filters().stream().map(f->new GovernedBiQueryService.Filter(f.column(),f.operator(),f.value())).toList(),
   r.orderBy()==null?List.of():r.orderBy().stream().map(s->new GovernedBiQueryService.Sort(s.column(),s.direction())).toList(),r.limit());
 }
 @GetMapping("/catalog/tables") public List<Map<String,Object>> tables(Authentication a){require(a,"bi:read");return bi.tables();}
 @PostMapping("/query") public Map<String,Object> query(@Valid @RequestBody QueryRequest r,Authentication a){require(a,"bi:query");return bi.query(tenant(a),a.getName(),definition(r));}
 @PostMapping("/query/drill-down") public Map<String,Object> drillDown(@Valid @RequestBody DrillDownRequest r,Authentication a){require(a,"bi:query");return bi.drillDown(tenant(a),a.getName(),definition(r.definition()),r.dimension(),r.value(),r.nextDimensions());}
 @PostMapping("/query/drill-through") public Map<String,Object> drillThrough(@Valid @RequestBody DrillThroughRequest r,Authentication a){require(a,"bi:query");List<GovernedBiQueryService.Filter> filters=r.filters()==null?List.of():r.filters().stream().map(f->new GovernedBiQueryService.Filter(f.column(),f.operator(),f.value())).toList();return bi.drillThrough(tenant(a),a.getName(),r.table(),r.columns(),filters,r.limit());}
 @PostMapping("/reports") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> createReport(@Valid @RequestBody ReportRequest r,Authentication a){require(a,"bi:write");return Map.of("id",bi.createReport(tenant(a),a.getName(),r.name(),r.description(),definition(r.definition())));}
 @GetMapping("/reports") public List<Map<String,Object>> reports(Authentication a){require(a,"bi:read");return bi.reports(tenant(a),a.getName());}
 @PostMapping("/reports/{id}/run") public Map<String,Object> runReport(@PathVariable UUID id,Authentication a){require(a,"bi:query");return bi.executeReport(tenant(a),a.getName(),id);}
 @PutMapping("/reports/{id}/shares") @ResponseStatus(HttpStatus.NO_CONTENT)
 public void shareReport(@PathVariable UUID id,@Valid @RequestBody ShareRequest r,Authentication a){require(a,"bi:write");bi.shareReport(tenant(a),a.getName(),id,r.subject(),r.permission().toUpperCase(Locale.ROOT));}
 @PostMapping("/dashboards") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> createDashboard(@Valid @RequestBody DashboardRequest r,Authentication a){require(a,"bi:write");return Map.of("id",bi.createDashboard(tenant(a),a.getName(),r.name(),r.description(),r.layout(),r.shared()));}
 @GetMapping("/dashboards") public List<Map<String,Object>> dashboards(Authentication a){require(a,"bi:read");return bi.dashboards(tenant(a),a.getName());}
 @GetMapping("/dashboards/{id}/widgets") public List<Map<String,Object>> widgets(@PathVariable UUID id,Authentication a){require(a,"bi:read");return bi.widgets(tenant(a),a.getName(),id);}
 @GetMapping("/dashboards/{id}/data") public List<Map<String,Object>> dashboardData(@PathVariable UUID id,Authentication a){require(a,"bi:query");return bi.dashboardData(tenant(a),a.getName(),id);}
 @PostMapping("/dashboards/{id}/widgets") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> addWidget(@PathVariable UUID id,@Valid @RequestBody WidgetRequest r,Authentication a){require(a,"bi:write");return Map.of("id",bi.addWidget(tenant(a),a.getName(),id,r.reportId(),r.title(),r.type().toUpperCase(Locale.ROOT),r.position(),r.options()));}
 @PutMapping("/dashboards/{id}/shares") @ResponseStatus(HttpStatus.NO_CONTENT)
 public void shareDashboard(@PathVariable UUID id,@Valid @RequestBody ShareRequest r,Authentication a){require(a,"bi:write");bi.shareDashboard(tenant(a),a.getName(),id,r.subject(),r.permission().toUpperCase(Locale.ROOT));}
 @GetMapping("/schedules") public List<Map<String,Object>> schedules(Authentication a){require(a,"bi:read");return bi.schedules(tenant(a),a.getName());}
 @PostMapping("/schedules") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> schedule(@Valid @RequestBody ScheduleRequest r,Authentication a){require(a,"bi:write");return Map.of("id",bi.schedule(tenant(a),a.getName(),r.reportId(),r.cronExpression(),r.timezone(),r.outputFormat().toUpperCase(Locale.ROOT),r.deliveryTarget()==null?"":r.deliveryTarget()));}
}
