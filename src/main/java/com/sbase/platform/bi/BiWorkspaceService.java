package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.*;
import java.util.*;

@Service
public class BiWorkspaceService {
 private final JdbcTemplate db;private final ObjectMapper mapper;private final GovernedBiQueryService queries;
 public BiWorkspaceService(@Qualifier("jdbcTemplate") JdbcTemplate db,ObjectMapper mapper,GovernedBiQueryService queries){this.db=db;this.mapper=mapper;this.queries=queries;}
 public List<Map<String,Object>> tables(){return queries.tables();}
 public Map<String,Object> query(UUID tenant,String subject,GovernedBiQueryService.QueryDefinition d){return queries.execute(tenant,subject,d);}
 @Transactional
 public UUID createReport(UUID tenant,String subject,String name,String description,GovernedBiQueryService.QueryDefinition definition){
  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_bi_reports(id,tenant_id,name,description,definition_json,owner_subject) VALUES(?,?,?,?,CAST(? AS jsonb),?)",id,tenant,name,description,write(definition),subject);return id;
 }
 public List<Map<String,Object>> reports(UUID tenant,String subject){
  return db.queryForList("""
   SELECT r.id,r.name,r.description,r.owner_subject,r.version,r.created_at,r.updated_at
   FROM sbase_bi_reports r WHERE r.tenant_id=? AND (r.owner_subject=? OR EXISTS(
    SELECT 1 FROM sbase_bi_report_grants g WHERE g.report_id=r.id AND g.subject=?))
   ORDER BY r.updated_at DESC LIMIT 500
   """,tenant,subject,subject);
 }
 public Map<String,Object> executeReport(UUID tenant,String subject,UUID id){
  Map<String,Object> row=reportRow(tenant,subject,id,"QUERY");try{return queries.execute(tenant,subject,mapper.readValue(row.get("definition_json").toString(),GovernedBiQueryService.QueryDefinition.class));}
  catch(Exception e){throw new IllegalArgumentException("Stored BI report definition is invalid",e);}
 }
 @Transactional
 public void shareReport(UUID tenant,String owner,UUID id,String subject,String permission){
  requireOwner("sbase_bi_reports",tenant,id,owner);if(!Set.of("READ","QUERY","EDIT","ADMIN").contains(permission))throw new IllegalArgumentException("Invalid report permission");
  db.update("INSERT INTO sbase_bi_report_grants(report_id,subject,permission,granted_by) VALUES(?,?,?,?) ON CONFLICT(report_id,subject) DO UPDATE SET permission=EXCLUDED.permission,granted_by=EXCLUDED.granted_by,created_at=now()",id,subject,permission,owner);
 }
 @Transactional
 public UUID createDashboard(UUID tenant,String subject,String name,String description,Map<String,Object> layout,boolean shared){
  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_bi_dashboards(id,tenant_id,name,description,layout_json,owner_subject,is_shared) VALUES(?,?,?, ?,CAST(? AS jsonb),?,?)",id,tenant,name,description,write(layout==null?Map.of():layout),subject,shared);return id;
 }
 public List<Map<String,Object>> dashboards(UUID tenant,String subject){
  return db.queryForList("""
   SELECT d.id,d.name,d.description,d.owner_subject,d.is_shared,d.version,d.updated_at
   FROM sbase_bi_dashboards d WHERE d.tenant_id=? AND (d.owner_subject=? OR d.is_shared OR EXISTS(
    SELECT 1 FROM sbase_bi_dashboard_grants g WHERE g.dashboard_id=d.id AND g.subject=?))
   ORDER BY d.updated_at DESC LIMIT 500
   """,tenant,subject,subject);
 }
 public List<Map<String,Object>> widgets(UUID tenant,String subject,UUID dashboard){
  dashboardRow(tenant,subject,dashboard,"READ");
  return db.queryForList("""
   SELECT w.id,w.report_id,w.title,w.widget_type,w.position_json,w.options_json
   FROM sbase_bi_widgets w JOIN sbase_bi_dashboards d ON d.id=w.dashboard_id
   WHERE d.id=? AND d.tenant_id=? ORDER BY w.created_at
   """,dashboard,tenant);
 }
 public List<Map<String,Object>> dashboardData(UUID tenant,String subject,UUID dashboard){
  List<Map<String,Object>> result=new ArrayList<>();
  for(Map<String,Object> widget:widgets(tenant,subject,dashboard)){
   Map<String,Object> item=new LinkedHashMap<>(widget);Object reportId=widget.get("report_id");
   if(reportId!=null){try{item.put("data",executeReport(tenant,subject,(UUID)reportId));}catch(Exception ex){item.put("dataError",ex.getMessage());}}
   result.add(item);
  }
  return result;
 }
 @Transactional
 public UUID addWidget(UUID tenant,String subject,UUID dashboard,UUID report,String title,String type,Map<String,Object> position,Map<String,Object> options){
  dashboardRow(tenant,subject,dashboard,"EDIT");
  if(report!=null)reportRow(tenant,subject,report,"READ");
  if(!Set.of("TABLE","BAR","LINE","PIE","KPI","AREA").contains(type))throw new IllegalArgumentException("Unsupported widget type");
  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_bi_widgets(id,dashboard_id,report_id,title,widget_type,position_json,options_json) VALUES(?,?,?,?,?,CAST(? AS jsonb),CAST(? AS jsonb))",id,dashboard,report,title,type,write(position==null?Map.of():position),write(options==null?Map.of():options));
  db.update("UPDATE sbase_bi_dashboards SET version=version+1,updated_at=now() WHERE id=?",dashboard);return id;
 }
 @Transactional
 public void shareDashboard(UUID tenant,String owner,UUID id,String subject,String permission){
  requireOwner("sbase_bi_dashboards",tenant,id,owner);if(!Set.of("READ","EDIT","ADMIN").contains(permission))throw new IllegalArgumentException("Invalid dashboard permission");
  db.update("INSERT INTO sbase_bi_dashboard_grants(dashboard_id,subject,permission,granted_by) VALUES(?,?,?,?) ON CONFLICT(dashboard_id,subject) DO UPDATE SET permission=EXCLUDED.permission,granted_by=EXCLUDED.granted_by,created_at=now()",id,subject,permission,owner);
 }
 public List<Map<String,Object>> schedules(UUID tenant,String subject){
  return db.queryForList("SELECT id,report_id,cron_expression,timezone,output_format,delivery_target,enabled,next_run_at,last_run_at,failure_count FROM sbase_bi_schedules WHERE tenant_id=? AND owner_subject=? ORDER BY next_run_at LIMIT 500",tenant,subject);
 }
 @Transactional
 public UUID schedule(UUID tenant,String subject,UUID report,String cron,String timezone,String format,String target){
  reportRow(tenant,subject,report,"QUERY");var expression=org.springframework.scheduling.support.CronExpression.parse(cron);ZoneId zone=ZoneId.of(timezone);ZonedDateTime next=expression.next(ZonedDateTime.now(zone));if(next==null)throw new IllegalArgumentException("Cron has no next occurrence");
  if(!Set.of("CSV","JSON").contains(format))throw new IllegalArgumentException("Output format must be CSV or JSON");
  if(target!=null&&target.length()>1000)throw new IllegalArgumentException("Delivery target too long");
  if(target!=null&&!target.isBlank()&&!target.startsWith("mailto:")&&target.contains("@"))target="mailto:"+target;\n  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_bi_schedules(id,tenant_id,report_id,owner_subject,cron_expression,timezone,output_format,delivery_target,next_run_at) VALUES(?,?,?,?,?,?,?,?,?)",id,tenant,report,subject,cron,timezone,format,target,next.toInstant());return id;
 }
 private Map<String,Object> reportRow(UUID tenant,String subject,UUID id,String needed){
  return db.query("""
   SELECT r.id,r.definition_json,r.owner_subject FROM sbase_bi_reports r
   WHERE r.id=? AND r.tenant_id=? AND (r.owner_subject=? OR EXISTS(
    SELECT 1 FROM sbase_bi_report_grants g WHERE g.report_id=r.id AND g.subject=?
    AND (g.permission='ADMIN' OR g.permission='EDIT' OR g.permission=? OR (?='QUERY' AND g.permission='READ')))
   )
   """,rs->{if(!rs.next())throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND,"BI report not found or access denied");Map<String,Object> x=new HashMap<>();x.put("id",rs.getObject("id"));x.put("definition_json",rs.getString("definition_json"));x.put("owner_subject",rs.getString("owner_subject"));return x;},id,tenant,subject,subject,needed,needed);
 }
 private void dashboardRow(UUID tenant,String subject,UUID id,String needed){
  Integer count=db.queryForObject("""
   SELECT count(*) FROM sbase_bi_dashboards d WHERE d.id=? AND d.tenant_id=? AND
   (d.owner_subject=? OR (d.is_shared AND ?='READ') OR EXISTS(
    SELECT 1 FROM sbase_bi_dashboard_grants g WHERE g.dashboard_id=d.id AND g.subject=?
    AND (g.permission='ADMIN' OR g.permission=? OR (?='READ' AND g.permission='EDIT'))))
   """,Integer.class,id,tenant,subject,needed,subject,needed,needed);
  if(count==null||count==0)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND,"Dashboard not found or access denied");
 }
 private void requireOwner(String table,UUID tenant,UUID id,String owner){
  if(!Set.of("sbase_bi_reports","sbase_bi_dashboards").contains(table))throw new IllegalArgumentException("Unsupported resource");
  Integer count=db.queryForObject("SELECT count(*) FROM "+table+" WHERE id=? AND tenant_id=? AND owner_subject=?",Integer.class,id,tenant,owner);
  if(count==null||count==0)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND,"Resource not found");
 }
 private String write(Object x){try{return mapper.writeValueAsString(x);}catch(Exception e){throw new IllegalArgumentException("Invalid JSON payload",e);}}
}
