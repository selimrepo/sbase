package com.sbase.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.bi.DorisBiQueryService;
import com.sbase.platform.security.PermissionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/platform/advanced")
public class AdvancedWorkspaceController {
 private static final Pattern IDENT=Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
 private final JdbcTemplate db; private final ObjectMapper json; private final DorisBiQueryService bi; private final PermissionService permissions;
 public AdvancedWorkspaceController(JdbcTemplate db,ObjectMapper json,DorisBiQueryService bi,PermissionService permissions){this.db=db;this.json=json;this.bi=bi;this.permissions=permissions;}

 public record ReportRequest(@NotBlank @Size(max=160) String name,String description,@NotBlank String definitionJson){}
 public record Metric(String column,String function,String alias){}
 public record Filter(String column,String operator,Object value){}
 public record ReportDefinition(String table,List<String> dimensions,List<Metric> metrics,List<Filter> filters,Integer limit){}
 public record DrilldownRequest(@NotBlank String column,Object value){}
 public record EvaluationRequest(UUID experimentId,@NotBlank @Size(max=160) String name,@NotBlank String taskType,List<Double> actual,List<Double> predicted){}
 public record LineageRequest(UUID sourceDatasetId,List<String> sourceColumns,List<UUID> parentFeatureIds,@NotBlank String transformationJson){}
 public record GrantRequest(@NotBlank @Size(max=200) String subject,@NotBlank String permission){}
 public record LifecycleRequest(@NotBlank String state){}
 public record DashboardRequest(@NotBlank @Size(max=160) String name,String description,String layoutJson){}
 public record WidgetRequest(@NotBlank String title,@NotBlank String widgetType,@NotBlank String queryJson,Integer x,Integer y,Integer width,Integer height,Integer refreshSeconds){}
 public record ScheduleUpdate(@NotBlank String cronExpression,@NotBlank String timezone,String calendarJson,UUID dependsOn,String failurePolicy,String misfirePolicy){}
 public record ShareRequest(@NotBlank @Size(max=200) String subject,@NotBlank String permission){}
 public record ReportScheduleRequest(@NotBlank String cronExpression,@NotBlank String timezone){}


 @GetMapping("/pipelines/{id}/versions") public List<Map<String,Object>> pipelineVersions(@PathVariable UUID id,Authentication a){permissions.require(a,"pipelines:read");return db.queryForList("SELECT * FROM pipeline_versions WHERE pipeline_id=? ORDER BY version DESC",id);}

 @PostMapping("/bi/reports") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> createReport(@Valid @RequestBody ReportRequest r,Authentication a){permissions.require(a,"bi:write");validate(parse(r.definitionJson()));UUID id=UUID.randomUUID();Instant now=Instant.now();db.update("INSERT INTO bi_reports(id,name,description,owner_subject,definition_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",id,r.name(),r.description(),a.getName(),r.definitionJson(),now,now);return Map.of("id",id,"name",r.name());}
 @GetMapping("/bi/reports")
 public Map<String,Object> reports(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="25") int size,@RequestParam(required=false) String search,Authentication a){
  permissions.require(a,"bi:read");if(page<0||size<1||size>100)throw new IllegalArgumentException("page must be >=0 and size must be 1..100");
  String term=search==null?"":search.trim();if(term.length()>160)throw new IllegalArgumentException("Search term is too long");
  String scope="(r.owner_subject=? OR EXISTS(SELECT 1 FROM bi_report_shares s WHERE s.report_id=r.id AND s.subject=?))";
  String filter=" AND (?='' OR lower(r.name) LIKE lower(?))";
  Integer total=db.queryForObject("SELECT count(*) FROM bi_reports r WHERE "+scope+filter,Integer.class,a.getName(),a.getName(),term,"%"+term+"%");
  List<Map<String,Object>> content=db.queryForList("SELECT r.*,(r.owner_subject=? OR EXISTS(SELECT 1 FROM bi_report_shares q WHERE q.report_id=r.id AND q.subject=? AND q.permission IN ('QUERY','EDIT'))) AS can_query,(r.owner_subject=?) AS can_manage FROM bi_reports r WHERE "+scope+filter+" ORDER BY r.updated_at DESC LIMIT ? OFFSET ?",a.getName(),a.getName(),a.getName(),a.getName(),term,"%"+term+"%",size,page*size);
  return Map.of("content",content,"page",page,"size",size,"totalElements",total==null?0:total,"totalPages",total==null?0:(int)Math.ceil(total/(double)size));
 }
 @PutMapping("/bi/reports/{id}") public void updateReport(@PathVariable UUID id,@Valid @RequestBody ReportRequest r,Authentication a){permissions.require(a,"bi:write");validate(parse(r.definitionJson()));if(db.update("UPDATE bi_reports SET name=?,description=?,definition_json=?,updated_at=now() WHERE id=? AND owner_subject=?",r.name(),r.description(),r.definitionJson(),id,a.getName())==0)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Report not found");}
 @DeleteMapping("/bi/reports/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) public void deleteReport(@PathVariable UUID id,Authentication a){permissions.require(a,"bi:write");db.update("DELETE FROM bi_reports WHERE id=? AND owner_subject=?",id,a.getName());}
 @PostMapping("/bi/reports/{id}/run") public Map<String,Object> runReport(@PathVariable UUID id,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="100") int size,Authentication a){permissions.require(a,"bi:query");return executePage(ownedReport(id,a.getName()),page,size);}
 @PostMapping("/bi/reports/{id}/drilldown") public Map<String,Object> drilldown(@PathVariable UUID id,@RequestBody DrilldownRequest d,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="100") int size,Authentication a){permissions.require(a,"bi:query");ReportDefinition base=ownedReport(id,a.getName());List<Filter> f=new ArrayList<>(base.filters()==null?List.of():base.filters());f.add(new Filter(d.column(),"EQ",d.value()));return executePage(new ReportDefinition(base.table(),base.dimensions(),base.metrics(),f,base.limit()),page,size);} 
 @PostMapping("/bi/reports/{id}/shares") @ResponseStatus(HttpStatus.CREATED)
 public void shareReport(@PathVariable UUID id,@Valid @RequestBody ShareRequest r,Authentication a){
  permissions.require(a,"bi:write");requireReportOwner(id,a.getName());String p=r.permission().toUpperCase(Locale.ROOT);
  if(!Set.of("READ","QUERY","EDIT").contains(p))throw new IllegalArgumentException("Share permission must be READ, QUERY or EDIT");
  if(a.getName().equals(r.subject()))throw new IllegalArgumentException("A report cannot be shared with its owner");
  db.update("INSERT INTO bi_report_shares(report_id,subject,permission,granted_by) VALUES(?,?,?,?) ON CONFLICT(report_id,subject) DO UPDATE SET permission=EXCLUDED.permission,granted_by=EXCLUDED.granted_by,created_at=now()",id,r.subject(),p,a.getName());
 }
 @GetMapping("/bi/reports/{id}/shares")
 public List<Map<String,Object>> reportShares(@PathVariable UUID id,Authentication a){permissions.require(a,"bi:write");requireReportOwner(id,a.getName());return db.queryForList("SELECT subject,permission,granted_by,created_at FROM bi_report_shares WHERE report_id=? ORDER BY subject",id);}
 @DeleteMapping("/bi/reports/{id}/shares/{subject}") @ResponseStatus(HttpStatus.NO_CONTENT)
 public void revokeReportShare(@PathVariable UUID id,@PathVariable String subject,Authentication a){permissions.require(a,"bi:write");requireReportOwner(id,a.getName());db.update("DELETE FROM bi_report_shares WHERE report_id=? AND subject=?",id,subject);}
 @PostMapping("/bi/reports/{id}/schedules") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> scheduleReport(@PathVariable UUID id,@Valid @RequestBody ReportScheduleRequest r,Authentication a){
  permissions.require(a,"bi:write");requireReportOwner(id,a.getName());ZoneId zone=ZoneId.of(r.timezone());var cron=org.springframework.scheduling.support.CronExpression.parse(r.cronExpression());var next=cron.next(java.time.ZonedDateTime.now(zone));if(next==null)throw new IllegalArgumentException("Cron has no next execution");
  UUID schedule=UUID.randomUUID();db.update("INSERT INTO bi_report_schedules(id,report_id,owner_subject,cron_expression,timezone,next_run_at) VALUES(?,?,?,?,?,?)",schedule,id,a.getName(),r.cronExpression(),zone.getId(),next.toInstant());return Map.of("id",schedule,"nextRunAt",next.toInstant(),"timezone",zone.getId());
 }
 @GetMapping("/bi/reports/{id}/schedules")
 public List<Map<String,Object>> reportSchedules(@PathVariable UUID id,Authentication a){permissions.require(a,"bi:write");requireReportOwner(id,a.getName());return db.queryForList("SELECT * FROM bi_report_schedules WHERE report_id=? AND owner_subject=? ORDER BY created_at DESC",id,a.getName());}
 @PostMapping("/bi/reports/{id}/schedules/{scheduleId}/pause")
 public void pauseReportSchedule(@PathVariable UUID id,@PathVariable UUID scheduleId,Authentication a){permissions.require(a,"bi:write");requireReportOwner(id,a.getName());db.update("UPDATE bi_report_schedules SET enabled=false,updated_at=now() WHERE id=? AND report_id=? AND owner_subject=?",scheduleId,id,a.getName());}
 @PostMapping("/bi/reports/{id}/schedules/{scheduleId}/resume")
 public void resumeReportSchedule(@PathVariable UUID id,@PathVariable UUID scheduleId,Authentication a){permissions.require(a,"bi:write");requireReportOwner(id,a.getName());db.update("UPDATE bi_report_schedules SET enabled=true,updated_at=now() WHERE id=? AND report_id=? AND owner_subject=?",scheduleId,id,a.getName());}
 @GetMapping("/bi/reports/{id}/schedules/{scheduleId}/runs")
 public List<Map<String,Object>> reportScheduleRuns(@PathVariable UUID id,@PathVariable UUID scheduleId,Authentication a){
  permissions.require(a,"bi:write");requireReportOwner(id,a.getName());
  return db.queryForList("SELECT * FROM bi_report_schedule_runs WHERE schedule_id=? AND EXISTS(SELECT 1 FROM bi_report_schedules s WHERE s.id=? AND s.report_id=?) ORDER BY started_at DESC LIMIT 100",scheduleId,scheduleId,id);
 }


 @PostMapping("/mining/evaluations") @ResponseStatus(HttpStatus.CREATED)
 public Map<String,Object> evaluate(@Valid @RequestBody EvaluationRequest r,Authentication a){permissions.require(a,"experiments:write");if(r.actual()==null||r.predicted()==null||r.actual().isEmpty()||r.actual().size()!=r.predicted().size()||r.actual().size()>1000000)throw new IllegalArgumentException("Actual/predicted arrays must have equal length in 1..1000000");if(r.actual().stream().anyMatch(v->v==null||!Double.isFinite(v))||r.predicted().stream().anyMatch(v->v==null||!Double.isFinite(v)))throw new IllegalArgumentException("Evaluation values must be finite");String task=r.taskType().toUpperCase(Locale.ROOT);if(!Set.of("REGRESSION","CLASSIFICATION").contains(task))throw new IllegalArgumentException("taskType must be REGRESSION or CLASSIFICATION");Map<String,Double> metrics=task.equals("REGRESSION")?regression(r.actual(),r.predicted()):classification(r.actual(),r.predicted());UUID id=UUID.randomUUID();try{db.update("INSERT INTO model_evaluations(id,experiment_id,name,task_type,metrics_json,sample_count,created_by) VALUES(?,?,?,?,?,?,?)",id,r.experimentId(),r.name(),task,json.writeValueAsString(metrics),r.actual().size(),a.getName());}catch(Exception e){throw new IllegalStateException("Cannot persist evaluation",e);}return Map.of("id",id,"taskType",task,"sampleCount",r.actual().size(),"metrics",metrics);}
 @GetMapping("/mining/evaluations") public List<Map<String,Object>> evaluations(@RequestParam(required=false) UUID experimentId,Authentication a){permissions.require(a,"experiments:read");return experimentId==null?db.queryForList("SELECT * FROM model_evaluations WHERE created_by=? ORDER BY created_at DESC LIMIT 500",a.getName()):db.queryForList("SELECT * FROM model_evaluations WHERE experiment_id=? AND created_by=? ORDER BY created_at DESC",experimentId,a.getName());}
 @GetMapping("/mining/evaluations/compare") public List<Map<String,Object>> compare(@RequestParam List<UUID> ids,Authentication a){permissions.require(a,"experiments:read");if(ids==null||ids.isEmpty()||ids.size()>50)throw new IllegalArgumentException("Provide 1..50 evaluation IDs");String marks=String.join(",",Collections.nCopies(ids.size(),"?"));List<Object> args=new ArrayList<>(ids);args.add(a.getName());return db.queryForList("SELECT * FROM model_evaluations WHERE id IN ("+marks+") AND created_by=?",args.toArray());}

 @GetMapping("/features/{id}/lineage") public List<Map<String,Object>> lineage(@PathVariable UUID id,Authentication a){permissions.require(a,"features:read");requireFeatureAccess(id,a,"READ");return db.queryForList("SELECT * FROM feature_lineage WHERE feature_set_id=? ORDER BY created_at DESC",id);}
 @PostMapping("/features/{id}/lineage") @ResponseStatus(HttpStatus.CREATED) public Map<String,Object> addLineage(@PathVariable UUID id,@Valid @RequestBody LineageRequest r,Authentication a){permissions.require(a,"features:write");requireFeatureAccess(id,a,"WRITE");UUID lid=UUID.randomUUID();try{db.update("INSERT INTO feature_lineage(id,feature_set_id,source_dataset_id,source_columns_json,parent_feature_ids_json,transformation_json,created_by) VALUES(?,?,?,?,?,?,?)",lid,id,r.sourceDatasetId(),json.writeValueAsString(r.sourceColumns()==null?List.of():r.sourceColumns()),json.writeValueAsString(r.parentFeatureIds()==null?List.of():r.parentFeatureIds()),r.transformationJson(),a.getName());}catch(Exception e){throw new IllegalStateException("Cannot persist feature lineage",e);}return Map.of("id",lid);}
 @PutMapping("/features/{id}/lifecycle") public void lifecycle(@PathVariable UUID id,@Valid @RequestBody LifecycleRequest r,Authentication a){permissions.require(a,"features:write");requireFeatureAccess(id,a,"WRITE");String s=r.state().toUpperCase(Locale.ROOT);if(!Set.of("ACTIVE","DEPRECATED","RETIRED").contains(s))throw new IllegalArgumentException("Lifecycle state must be ACTIVE, DEPRECATED or RETIRED");if(db.update("UPDATE feature_sets SET lifecycle_state=?,updated_at=now() WHERE id=?",s,id)==0)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Feature set not found");}
 @PostMapping("/features/{id}/grants") public void grant(@PathVariable UUID id,@Valid @RequestBody GrantRequest r,Authentication a){
  permissions.require(a,"features:write");requireFeatureOwnerOrAdmin(id,a);
  String p=r.permission().toUpperCase(Locale.ROOT);if(!Set.of("READ","WRITE","ADMIN").contains(p))throw new IllegalArgumentException("Grant must be READ, WRITE or ADMIN");
  db.update("INSERT INTO feature_access_grants(feature_set_id,subject,permission,granted_by) VALUES(?,?,?,?) ON CONFLICT(feature_set_id,subject) DO UPDATE SET permission=EXCLUDED.permission,granted_by=EXCLUDED.granted_by,created_at=now()",id,r.subject(),p,a.getName());
 }
 @GetMapping("/features/{id}/grants") public List<Map<String,Object>> grants(@PathVariable UUID id,Authentication a){permissions.require(a,"features:read");requireFeatureOwnerOrAdmin(id,a);return db.queryForList("SELECT * FROM feature_access_grants WHERE feature_set_id=? ORDER BY subject",id);}
 @DeleteMapping("/features/{id}/grants/{subject}") public void revokeGrant(@PathVariable UUID id,@PathVariable String subject,Authentication a){permissions.require(a,"features:write");requireFeatureOwnerOrAdmin(id,a);db.update("DELETE FROM feature_access_grants WHERE feature_set_id=? AND subject=?",id,subject);}


 @PostMapping("/dashboards") @ResponseStatus(HttpStatus.CREATED) public Map<String,Object> createDashboard(@Valid @RequestBody DashboardRequest r,Authentication a){permissions.require(a,"bi:write");UUID id=UUID.randomUUID();Instant now=Instant.now();db.update("INSERT INTO dashboards(id,name,description,layout_json,owner_subject,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",id,r.name(),r.description(),r.layoutJson()==null?"{}":r.layoutJson(),a.getName(),now,now);return Map.of("id",id,"version",1);}
 @GetMapping("/dashboards") public List<Map<String,Object>> dashboards(Authentication a){permissions.require(a,"bi:read");return db.queryForList("SELECT * FROM dashboards WHERE owner_subject=? ORDER BY updated_at DESC",a.getName());}
 @GetMapping("/dashboards/{id}/widgets") public List<Map<String,Object>> widgets(@PathVariable UUID id,Authentication a){permissions.require(a,"bi:read");ownedDashboard(id,a.getName());return db.queryForList("SELECT * FROM dashboard_widgets WHERE dashboard_id=? ORDER BY position_y,position_x",id);}
 @PostMapping("/dashboards/{id}/widgets") @ResponseStatus(HttpStatus.CREATED) public Map<String,Object> addWidget(@PathVariable UUID id,@Valid @RequestBody WidgetRequest w,Authentication a){permissions.require(a,"bi:write");ownedDashboard(id,a.getName());String type=w.widgetType().toUpperCase(Locale.ROOT);if(!Set.of("KPI","TABLE","BAR","LINE","PIE","AREA").contains(type))throw new IllegalArgumentException("Unsupported widget type");validate(parse(w.queryJson()));int x=bounded(w.x(),0,0,48),y=bounded(w.y(),0,0,10000),width=bounded(w.width(),6,1,48),height=bounded(w.height(),4,1,100),refresh=bounded(w.refreshSeconds(),0,0,86400);UUID wid=UUID.randomUUID();db.update("INSERT INTO dashboard_widgets(id,dashboard_id,title,widget_type,query_json,position_x,position_y,width,height,data_source,refresh_seconds) VALUES(?,?,?,?,?,?,?,?,?,'BI_REPORT',?)",wid,id,w.title(),type,w.queryJson(),x,y,width,height,refresh);db.update("UPDATE dashboards SET version=version+1,updated_at=now() WHERE id=?",id);return Map.of("id",wid,"dashboardId",id);}
 @PostMapping("/dashboards/{id}/widgets/{wid}/run") public Map<String,Object> runWidget(@PathVariable UUID id,@PathVariable UUID wid,Authentication a){permissions.require(a,"bi:query");ownedDashboard(id,a.getName());String spec=db.query("SELECT query_json FROM dashboard_widgets WHERE id=? AND dashboard_id=?",rs->rs.next()?rs.getString(1):null,wid,id);if(spec==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Widget not found");return execute(parse(spec));}

 @PutMapping("/schedules/{id}") public Map<String,Object> updateSchedule(@PathVariable UUID id,@Valid @RequestBody ScheduleUpdate r,Authentication a){
  permissions.require(a,"pipelines:write");requireScheduleOwner(id,a);
  ZoneId zone;try{zone=ZoneId.of(r.timezone());}catch(Exception e){throw new IllegalArgumentException("Timezone must be a valid IANA zone",e);}
  var cron=org.springframework.scheduling.support.CronExpression.parse(r.cronExpression());
  var next=cron.next(java.time.ZonedDateTime.now(zone));if(next==null)throw new IllegalArgumentException("Cron has no next execution");
  String fp=Optional.ofNullable(r.failurePolicy()).orElse("RETRY").toUpperCase(Locale.ROOT),mp=Optional.ofNullable(r.misfirePolicy()).orElse("RUN_ONCE").toUpperCase(Locale.ROOT);
  if(!Set.of("RETRY","DISABLE","CONTINUE").contains(fp)||!Set.of("RUN_ONCE","SKIP","CATCH_UP").contains(mp))throw new IllegalArgumentException("Invalid scheduler policy");
  if(Objects.equals(id,r.dependsOn()))throw new IllegalArgumentException("Schedule cannot depend on itself");
  if(r.dependsOn()!=null){
   if(!Boolean.TRUE.equals(db.queryForObject("SELECT EXISTS(SELECT 1 FROM scheduled_jobs WHERE id=?)",Boolean.class,r.dependsOn())))throw new IllegalArgumentException("Dependency schedule does not exist");
   Boolean cycle=db.queryForObject("""
    WITH RECURSIVE dependency_chain(id,depends_on) AS (
     SELECT id,depends_on FROM scheduled_jobs WHERE id=?
     UNION
     SELECT parent.id,parent.depends_on FROM scheduled_jobs parent JOIN dependency_chain child ON parent.id=child.depends_on
    )
    SELECT EXISTS(SELECT 1 FROM dependency_chain WHERE id=?)
    """,Boolean.class,r.dependsOn(),id);
   if(Boolean.TRUE.equals(cycle))throw new IllegalArgumentException("Schedule dependency would create a cycle");
  }
  String calendar=validateCalendar(r.calendarJson());
  db.update("UPDATE scheduled_jobs SET cron_expression=?,timezone=?,calendar_json=?,depends_on=?,failure_policy=?,misfire_policy=?,next_run_at=?,updated_at=now() WHERE id=?",r.cronExpression(),zone.getId(),calendar,r.dependsOn(),fp,mp,next.toInstant(),id);
  return Map.of("id",id,"nextRunAt",next.toInstant(),"timezone",zone.getId());
 }
 @PostMapping("/schedules/{id}/pause") public void pause(@PathVariable UUID id,Authentication a){permissions.require(a,"pipelines:write");requireScheduleOwner(id,a);db.update("UPDATE scheduled_jobs SET enabled=false,updated_at=now() WHERE id=?",id);}
 @PostMapping("/schedules/{id}/resume") public void resume(@PathVariable UUID id,Authentication a){permissions.require(a,"pipelines:write");requireScheduleOwner(id,a);db.update("UPDATE scheduled_jobs SET enabled=true,consecutive_failures=0,updated_at=now() WHERE id=?",id);}
 @GetMapping("/schedules/{id}/runs") public List<Map<String,Object>> scheduleRuns(@PathVariable UUID id,Authentication a){permissions.require(a,"pipelines:read");requireScheduleOwner(id,a);return db.queryForList("SELECT * FROM schedule_runs WHERE schedule_id=? ORDER BY created_at DESC LIMIT 200",id);}

 private void requireFeatureOwnerOrAdmin(UUID id,Authentication auth){
  String owner=db.query("SELECT owner_subject FROM feature_sets WHERE id=?",rs->rs.next()?rs.getString(1):null,id);
  if(owner==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Feature set not found");
  boolean admin=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN")||a.getAuthority().equals("PERM_platform:admin"))||permissions.has(auth.getName(),"platform:admin");
  if(!admin&&!owner.equals(auth.getName()))throw new org.springframework.security.access.AccessDeniedException("Only the feature-set owner or platform administrator can manage grants");
 }
 private String validateCalendar(String source){
  String value=source==null?"{}":source;
  if(value.length()>50000)throw new IllegalArgumentException("Calendar JSON is too large");
  try{
   var calendar=json.readTree(value);if(calendar==null||!calendar.isObject())throw new IllegalArgumentException("Calendar must be a JSON object");
   for(String key:List.of("skipDates","includeDates")){
    var dates=calendar.get(key);if(dates==null)continue;if(!dates.isArray()||dates.size()>1000)throw new IllegalArgumentException(key+" must be an array of at most 1000 dates");
    for(var date:dates){if(!date.isTextual())throw new IllegalArgumentException(key+" values must be ISO dates");java.time.LocalDate.parse(date.asText());}
   }
   var weekdays=calendar.get("skipWeekdays");if(weekdays!=null){if(!weekdays.isArray()||weekdays.size()>7)throw new IllegalArgumentException("skipWeekdays must be an array");for(var day:weekdays){if(!day.isTextual())throw new IllegalArgumentException("Weekday values must be names");java.time.DayOfWeek.valueOf(day.asText().toUpperCase(Locale.ROOT));}}
   return value;
  }catch(Exception ex){throw new IllegalArgumentException("Invalid schedule calendar JSON: "+ex.getMessage(),ex);}
 }
 private void requireScheduleOwner(UUID id,Authentication auth){String owner=db.query("SELECT created_by FROM scheduled_jobs WHERE id=?",rs->rs.next()?rs.getString(1):null,id);boolean admin=auth.getAuthorities().stream().anyMatch(x->x.getAuthority().equals("ROLE_ADMIN")||x.getAuthority().equals("PERM_platform:admin"))||permissions.has(auth.getName(),"platform:admin");if(owner==null){if(!admin)throw new org.springframework.security.access.AccessDeniedException("Legacy schedule is restricted to platform administrators");return;}if(!owner.equals(auth.getName())&&!admin)throw new org.springframework.security.access.AccessDeniedException("Schedule belongs to another subject");}
 private void requireReportOwner(UUID id,String owner){if(db.queryForObject("SELECT count(*) FROM bi_reports WHERE id=? AND owner_subject=?",Integer.class,id,owner)==0)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Report not found");}
 private ReportDefinition ownedReport(UUID id,String owner){String s=db.query("SELECT r.definition_json FROM bi_reports r WHERE r.id=? AND (r.owner_subject=? OR EXISTS(SELECT 1 FROM bi_report_shares sh WHERE sh.report_id=r.id AND sh.subject=? AND sh.permission IN ('QUERY','EDIT')))",rs->rs.next()?rs.getString(1):null,id,owner,owner);if(s==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Report not found or query access denied");return parse(s);}
 private ReportDefinition parse(String s){try{return json.readValue(s,ReportDefinition.class);}catch(Exception e){throw new IllegalArgumentException("Invalid report definition JSON",e);}}
 private void validate(ReportDefinition d){if(d==null||d.table()==null||!IDENT.matcher(d.table()).matches())throw new IllegalArgumentException("Invalid Doris table");if(d.dimensions()==null||d.dimensions().size()>20)throw new IllegalArgumentException("Up to 20 dimensions are supported");d.dimensions().forEach(this::identifier);if(d.metrics()==null||d.metrics().isEmpty()||d.metrics().size()>20)throw new IllegalArgumentException("1..20 metrics required");d.metrics().forEach(m->{identifier(m.column());identifier(m.alias());if(!Set.of("COUNT","SUM","AVG","MIN","MAX").contains(m.function().toUpperCase(Locale.ROOT)))throw new IllegalArgumentException("Invalid aggregate function");});if(d.filters()!=null){if(d.filters().size()>50)throw new IllegalArgumentException("Too many filters");d.filters().forEach(f->{identifier(f.column());if(!Set.of("EQ","NE","GT","GTE","LT","LTE","CONTAINS","IN","BETWEEN","IS_NULL","NOT_NULL","STARTS_WITH","ENDS_WITH").contains(f.operator().toUpperCase(Locale.ROOT)))throw new IllegalArgumentException("Invalid filter operator");if("IN".equalsIgnoreCase(f.operator())&&(!(f.value() instanceof List<?> values)||values.isEmpty()||values.size()>100))throw new IllegalArgumentException("IN filter requires 1..100 values");if("BETWEEN".equalsIgnoreCase(f.operator())&&(!(f.value() instanceof List<?> values)||values.size()!=2))throw new IllegalArgumentException("BETWEEN filter requires two values");});}if(d.limit()!=null&&(d.limit()<1||d.limit()>10000))throw new IllegalArgumentException("Limit must be 1..10000");}
 private Map<String,Object> execute(ReportDefinition d){return executePage(d,0,100);}
 private Map<String,Object> executePage(ReportDefinition d,int page,int size){
  validate(d);if(page<0||size<1||size>500||page>10000)throw new IllegalArgumentException("page must be 0..10000 and size 1..500");
  List<String> select=new ArrayList<>(d.dimensions());for(Metric m:d.metrics())select.add(m.function().toUpperCase(Locale.ROOT)+"("+m.column()+") AS "+m.alias());
  StringBuilder sql=new StringBuilder("SELECT ").append(String.join(",",select)).append(" FROM ").append(d.table());List<String> where=new ArrayList<>();if(d.filters()!=null)for(Filter f:d.filters())where.add(filterSql(f));
  if(!where.isEmpty())sql.append(" WHERE ").append(String.join(" AND ",where));if(!d.dimensions().isEmpty())sql.append(" GROUP BY ").append(String.join(",",d.dimensions()));
  int maxRows=d.limit()==null?10000:Math.min(10000,d.limit());sql.append(" LIMIT ").append(maxRows);
  List<Map<String,Object>> all=bi.query(sql.toString());int from=Math.min(all.size(),page*size),to=Math.min(all.size(),from+size);
  return Map.of("rows",all.subList(from,to),"page",page,"size",size,"totalRows",all.size(),"hasNext",to<all.size(),"truncated",all.size()==maxRows,"sql",sql.toString());
 }
 private String filterSql(Filter f){
  String op=f.operator().toUpperCase(Locale.ROOT);
  String column=f.column();
  return switch(op){
   case "IS_NULL" -> column+" IS NULL";
   case "NOT_NULL" -> column+" IS NOT NULL";
   case "IN" -> {List<?> values=(List<?>)f.value();yield column+" IN ("+String.join(",",values.stream().map(v->literal(v,"EQ")).toList())+")";}
   case "BETWEEN" -> {List<?> values=(List<?>)f.value();yield column+" BETWEEN "+literal(values.get(0),"EQ")+" AND "+literal(values.get(1),"EQ");}
   case "STARTS_WITH" -> column+" LIKE "+literal(String.valueOf(f.value())+"%","EQ");
   case "ENDS_WITH" -> column+" LIKE "+literal("%"+String.valueOf(f.value()),"EQ");
   default -> column+" "+operator(op)+" "+literal(f.value(),op);
  };
 }
 private String operator(String s){return switch(s.toUpperCase(Locale.ROOT)){case "EQ"->"=";case "NE"->"<>";case "GT"->">";case "GTE"->">=";case "LT"->"<";case "LTE"->"<=";case "CONTAINS","STARTS_WITH","ENDS_WITH"->"LIKE";default->throw new IllegalArgumentException("Invalid operator");};}
 private String literal(Object v,String op){if(v==null)return "NULL";if(v instanceof Number n){if(!Double.isFinite(n.doubleValue()))throw new IllegalArgumentException("Non-finite filter number");return n.toString();}if(v instanceof Boolean b)return b?"TRUE":"FALSE";String s=String.valueOf(v);if(s.length()>2000)throw new IllegalArgumentException("Filter too long");if("CONTAINS".equalsIgnoreCase(op))s="%"+s+"%";return "'"+s.replace("'","''")+"'";}
 private void identifier(String s){if(s==null||!IDENT.matcher(s).matches())throw new IllegalArgumentException("Unsafe identifier");}
 private int bounded(Integer v,int def,int min,int max){int n=v==null?def:v;if(n<min||n>max)throw new IllegalArgumentException("Value must be "+min+".."+max);return n;}
 private Map<String,Object> ownedDashboard(UUID id,String owner){return db.query("SELECT * FROM dashboards WHERE id=? AND owner_subject=?",rs->{if(!rs.next())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Dashboard not found");Map<String,Object> m=new LinkedHashMap<>();var md=rs.getMetaData();for(int i=1;i<=md.getColumnCount();i++)m.put(md.getColumnLabel(i),rs.getObject(i));return m;},id,owner);}
 private void requireFeatureAccess(UUID id,Authentication auth,String wanted){
  String subject=auth.getName();
  Map<String,Object> feature=db.query("SELECT lifecycle_state,owner_subject FROM feature_sets WHERE id=?",rs->{if(!rs.next())return null;Map<String,Object> m=new HashMap<>();m.put("lifecycle",rs.getString(1));m.put("owner",rs.getString(2));return m;},id);
  if(feature==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Feature set not found");
  String lifecycle=(String)feature.get("lifecycle"),owner=(String)feature.get("owner");
  if("WRITE".equals(wanted)&&!"ACTIVE".equalsIgnoreCase(lifecycle))throw new org.springframework.security.access.AccessDeniedException("Only ACTIVE feature sets can be changed");
  boolean admin=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN")||a.getAuthority().equals("PERM_platform:admin"))||permissions.has(subject,"platform:admin");
  if(subject.equals(owner)||admin)return;
  Integer count=db.queryForObject("SELECT count(*) FROM feature_access_grants WHERE feature_set_id=?",Integer.class,id);
  if((count==null||count==0)&&owner==null)return;
  if(count==null||count==0)throw new org.springframework.security.access.AccessDeniedException("Feature-set owner has not granted access");
  String grant=db.query("SELECT permission FROM feature_access_grants WHERE feature_set_id=? AND subject=?",rs->rs.next()?rs.getString(1):null,id,subject);
  if(!"ADMIN".equals(grant)&&!("WRITE".equals(grant))&&!("READ".equals(wanted)&&"READ".equals(grant)))throw new org.springframework.security.access.AccessDeniedException("Feature access denied");
 }
 private Map<String,Double> regression(List<Double>a,List<Double>p){int n=a.size();double mae=0,mse=0,mean=a.stream().mapToDouble(Double::doubleValue).average().orElse(0),sst=0;for(int i=0;i<n;i++){double e=a.get(i)-p.get(i);mae+=Math.abs(e);mse+=e*e;sst+=(a.get(i)-mean)*(a.get(i)-mean);}Map<String,Double>m=new LinkedHashMap<>();m.put("mae",mae/n);m.put("mse",mse/n);m.put("rmse",Math.sqrt(mse/n));m.put("r2",sst==0?0:1-mse/sst);return m;}
 private Map<String,Double> classification(List<Double>a,List<Double>p){long tp=0,tn=0,fp=0,fn=0;for(int i=0;i<a.size();i++){boolean x=a.get(i)>=.5,y=p.get(i)>=.5;if(x&&y)tp++;else if(!x&&!y)tn++;else if(y)fp++;else fn++;}double pr=tp+fp==0?0:(double)tp/(tp+fp),re=tp+fn==0?0:(double)tp/(tp+fn);Map<String,Double>m=new LinkedHashMap<>();m.put("accuracy",(double)(tp+tn)/a.size());m.put("precision",pr);m.put("recall",re);m.put("f1",pr+re==0?0:2*pr*re/(pr+re));return m;}
}
