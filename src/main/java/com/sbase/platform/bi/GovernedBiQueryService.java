package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Service
public class GovernedBiQueryService {
 private final DorisBiQueryService doris;private final DorisTenantTablePolicy tablePolicy;private final JdbcTemplate control;private final JdbcTemplate analytics;
 private final ObjectMapper mapper;private final String database;private final Set<String> tables;
 private final Semaphore permits;private final long cacheMillis;private final int maxRows;private final double maxCost;private final ConcurrentHashMap<String,CacheEntry> cache=new ConcurrentHashMap<>();
 public GovernedBiQueryService(DorisBiQueryService doris,DorisTenantTablePolicy tablePolicy,@Qualifier("jdbcTemplate") JdbcTemplate control,
  @Qualifier("dorisJdbcTemplate") JdbcTemplate analytics,ObjectMapper mapper,
  @Value("${sbase.doris.database:analytics}") String database,@Value("${sbase.doris.allowed-tables:}") String allowed,
  @Value("${sbase.bi.max-concurrent-queries:4}") int concurrency,@Value("${sbase.bi.cache-ttl-ms:30000}") long cacheMillis,
  @Value("${sbase.bi.max-rows:5000}") int maxRows,@Value("${sbase.bi.max-estimated-cost:100000}") double maxCost){
  this.doris=doris;this.tablePolicy=tablePolicy;this.control=control;this.analytics=analytics;this.mapper=mapper;this.database=database;
  this.tables=new HashSet<>();for(String t:allowed.split(","))if(!t.isBlank())tables.add(t.trim().toLowerCase(Locale.ROOT));
  this.permits=new Semaphore(Math.max(1,concurrency));this.cacheMillis=Math.max(0,cacheMillis);this.maxRows=Math.min(10000,Math.max(1,maxRows));this.maxCost=Math.max(1000,maxCost);
 }
 public List<Map<String,Object>> tables(){List<Map<String,Object>> out=new ArrayList<>();for(String t:tables){String governed;try{governed=tablePolicy.requireTenantScoped(t);}catch(org.springframework.security.access.AccessDeniedException denied){continue;}String[] parts=tableParts(governed);List<Map<String,Object>> cols=analytics.queryForList("SELECT COLUMN_NAME AS name,DATA_TYPE AS type FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",parts[0],parts[1]);out.add(Map.of("name",t,"columns",cols));}return out;}
 public Map<String,Object> execute(UUID tenant,String subject,QueryDefinition d){
  long started=System.nanoTime();String hash=hash(write(d));String key=tenant+":"+hash;CacheEntry cached=cache.get(key);
  if(cached!=null&&cached.expiresAt()>System.currentTimeMillis()){audit(tenant,subject,hash,0,cached.rows().size(),0,true,"CACHE_HIT");return Map.of("rows",cached.rows(),"rowCount",cached.rows().size(),"cacheHit",true,"queryHash",hash);}
  if(!permits.tryAcquire())throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,"BI query concurrency limit reached");
  String sql;try{sql=compile(d,tenant);double planned=(double)(d.limit()==null?1000:d.limit())*(1+(d.dimensions()==null?0:d.dimensions().size())+(d.filters()==null?0:d.filters().size()));if(planned>maxCost)throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,"Estimated BI query cost exceeds configured budget");}catch(RuntimeException e){permits.release();throw e;}
  try{
   List<Map<String,Object>> rows=doris.query(tenant,sql);long elapsed=(System.nanoTime()-started)/1_000_000;
   double cost=estimate(d,rows.size(),elapsed);audit(tenant,subject,hash,elapsed,rows.size(),cost,false,"SUCCEEDED");
   if(cacheMillis>0){cache.entrySet().removeIf(e->e.getValue().expiresAt()<System.currentTimeMillis());if(cache.size()>=2000){String oldest=cache.keySet().stream().findFirst().orElse(null);if(oldest!=null)cache.remove(oldest);}cache.put(key,new CacheEntry(List.copyOf(rows),System.currentTimeMillis()+cacheMillis));}
   return Map.of("rows",rows,"rowCount",rows.size(),"cacheHit",false,"queryHash",hash,"elapsedMs",elapsed,"estimatedCost",cost);
  }catch(RuntimeException ex){long elapsed=(System.nanoTime()-started)/1_000_000;audit(tenant,subject,hash,elapsed,0,0,false,"FAILED");throw ex;}
  finally{permits.release();}
 }
 public String compile(QueryDefinition d,UUID tenant){
  if(d==null||d.table()==null||!tables.contains(d.table().toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Table is not allow-listed");
  if(d.dimensions()==null||d.dimensions().size()>12||d.metrics()==null||d.metrics().isEmpty()||d.metrics().size()>20)throw new IllegalArgumentException("Select up to 12 dimensions and 1..20 metrics");
  String governedTable=tablePolicy.requireTenantScoped(d.table());
  String[] tableParts=tableParts(governedTable);Set<String> valid=new HashSet<>();List<Map<String,Object>> cols=analytics.queryForList("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",tableParts[0],tableParts[1]);
  cols.forEach(c->valid.add(Objects.toString(c.get("COLUMN_NAME"),"").toLowerCase(Locale.ROOT)));
  List<String> select=new ArrayList<>(),groups=new ArrayList<>(),aliases=new ArrayList<>();
  for(String dim:d.dimensions()){column(dim,valid);select.add(dim);groups.add(dim);aliases.add(dim);}
  for(Metric m:d.metrics()){String fn=m.aggregate()==null?"COUNT":m.aggregate().toUpperCase(Locale.ROOT);if(!Set.of("COUNT","SUM","AVG","MIN","MAX","COUNT_DISTINCT").contains(fn))throw new IllegalArgumentException("Unsupported metric aggregation");if(!("*".equals(m.column())&&"COUNT".equals(fn)))column(m.column(),valid);String alias=identifier(m.alias());String expression=fn.equals("COUNT_DISTINCT")?"COUNT(DISTINCT "+m.column()+")":fn+"("+m.column()+")";select.add(expression+" AS "+alias);aliases.add(alias);}
  if(select.isEmpty())throw new IllegalArgumentException("At least one dimension or metric is required");
  StringBuilder sql=new StringBuilder("SELECT ").append(String.join(",",select)).append(" FROM ").append(governedTable);
  if(!valid.contains("tenant_id"))throw new org.springframework.security.access.AccessDeniedException("BI source table must expose tenant_id for tenant isolation");
  List<String> where=new ArrayList<>();if(d.filters()!=null){if(d.filters().size()>40)throw new IllegalArgumentException("At most 40 filters are allowed");for(Filter f:d.filters())where.add(filter(f,valid));}
  if(!where.isEmpty())sql.append(" WHERE ").append(String.join(" AND ",where));
  if(!groups.isEmpty())sql.append(" GROUP BY ").append(String.join(",",groups));
  if(d.orderBy()!=null&&!d.orderBy().isEmpty()){List<String> order=new ArrayList<>();if(d.orderBy().size()>10)throw new IllegalArgumentException("At most 10 sort expressions");for(Sort s:d.orderBy()){String col=identifier(s.column());if(!aliases.contains(col))throw new IllegalArgumentException("Sort must reference a selected dimension or metric alias");String dir="DESC".equalsIgnoreCase(s.direction())?"DESC":"ASC";order.add(col+" "+dir);}sql.append(" ORDER BY ").append(String.join(",",order));}
  int limit=d.limit()==null?1000:d.limit();if(limit<1||limit>maxRows)throw new IllegalArgumentException("Query row limit must be 1.."+maxRows);sql.append(" LIMIT ").append(limit);return sql.toString();
 }
 public Map<String,Object> drillDown(UUID tenant,String subject,QueryDefinition base,String dimension,Object value,List<String> nextDimensions){
  if(base.dimensions()==null||!base.dimensions().contains(dimension))throw new IllegalArgumentException("Drill-down dimension must be selected in the parent query");
  List<Filter> filters=new ArrayList<>(base.filters()==null?List.of():base.filters());filters.add(new Filter(dimension,value==null?"IS_NULL":"EQ",value));
  QueryDefinition child=new QueryDefinition(base.table(),nextDimensions==null?base.dimensions():nextDimensions,base.metrics(),filters,base.orderBy(),base.limit());
  Map<String,Object> result=new LinkedHashMap<>(execute(tenant,subject,child));result.put("drillDown",Map.of("dimension",dimension,"value",String.valueOf(value)));return result;
 }
 public Map<String,Object> drillThrough(UUID tenant,String subject,String table,List<String> columns,List<Filter> filters,Integer requestedLimit){
  if(table==null||!tables.contains(table.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Table is not allow-listed");
  if(columns==null||columns.isEmpty()||columns.size()>30)throw new IllegalArgumentException("Select 1..30 drill-through columns");
  String governedTable=tablePolicy.requireTenantScoped(table);
  String[] parts=tableParts(governedTable);Set<String> valid=new HashSet<>();
  analytics.queryForList("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",parts[0],parts[1])
   .forEach(row->valid.add(Objects.toString(row.get("COLUMN_NAME"),"").toLowerCase(Locale.ROOT)));
  if(!valid.contains("tenant_id"))throw new org.springframework.security.access.AccessDeniedException("BI source table must expose tenant_id for tenant isolation");
  columns.forEach(col->column(col,valid));int limit=requestedLimit==null?500:requestedLimit;
  if(limit<1||limit>Math.min(1000,maxRows))throw new IllegalArgumentException("Drill-through limit must be 1..1000");
  List<String> where=new ArrayList<>();
  if(filters!=null){if(filters.size()>30)throw new IllegalArgumentException("At most 30 drill-through filters");for(Filter f:filters)where.add(filter(f,valid));}
  String sql="SELECT "+String.join(",",columns)+" FROM "+governedTable
      +(where.isEmpty()?"":" WHERE "+String.join(" AND ",where))+" LIMIT "+limit;
  String hash=hash(sql);if(!permits.tryAcquire())throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,"BI query concurrency limit reached");
  long started=System.nanoTime();try{List<Map<String,Object>> rows=doris.query(tenant,sql);long elapsed=(System.nanoTime()-started)/1_000_000;audit(tenant,subject,hash,elapsed,rows.size(),rows.size()+elapsed/1000.0,false,"SUCCEEDED");return Map.of("rows",rows,"rowCount",rows.size(),"queryHash",hash,"elapsedMs",elapsed,"drillThrough",true);}
  catch(RuntimeException ex){long elapsed=(System.nanoTime()-started)/1_000_000;audit(tenant,subject,hash,elapsed,0,0,false,"FAILED");throw ex;}
  finally{permits.release();}
 }
 private String filter(Filter f,Set<String> valid){column(f.column(),valid);String op=f.operator().toUpperCase(Locale.ROOT);Object v=f.value();
  return switch(op){case "IS_NULL"->f.column()+" IS NULL";case "NOT_NULL"->f.column()+" IS NOT NULL";
   case "EQ","NE","GT","GTE","LT","LTE"->{String symbol=switch(op){case "EQ"->"=";case "NE"->"<>";case "GT"->">";case "GTE"->">=";case "LT"->"<";default->"<=";};yield f.column()+" "+symbol+" "+literal(v);}
   case "CONTAINS","STARTS_WITH","ENDS_WITH"->{String s=String.valueOf(v);s=s.replace("\\","\\\\").replace("%","\\%").replace("_","\\_");if(op.equals("CONTAINS"))s="%"+s+"%";if(op.equals("STARTS_WITH"))s+="%";if(op.equals("ENDS_WITH"))s="%"+s;yield f.column()+" LIKE "+literal(s);}
   case "IN"->{if(!(v instanceof List<?> list)||list.isEmpty()||list.size()>100)throw new IllegalArgumentException("IN requires 1..100 values");yield f.column()+" IN ("+String.join(",",list.stream().map(this::literal).toList())+")";}
   case "BETWEEN"->{if(!(v instanceof List<?> list)||list.size()!=2)throw new IllegalArgumentException("BETWEEN requires two values");yield f.column()+" BETWEEN "+literal(list.get(0))+" AND "+literal(list.get(1));}
   default->throw new IllegalArgumentException("Unsupported filter operator");};
 }
 private String literal(Object v){if(v==null)return "NULL";if(v instanceof Number n){double d=n.doubleValue();if(!Double.isFinite(d))throw new IllegalArgumentException("Non-finite filter value");return n.toString();}if(v instanceof Boolean b)return b?"TRUE":"FALSE";String s=String.valueOf(v);if(s.length()>2000)throw new IllegalArgumentException("Filter value too long");return "'"+s.replace("\\","\\\\").replace("'","''")+"'";}
 private void column(String s,Set<String> valid){identifier(s);if("tenant_id".equalsIgnoreCase(s))throw new IllegalArgumentException("tenant_id is platform-managed and cannot be selected or filtered");if(!valid.contains(s.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Column is not present in selected table: "+s);}
 private String[] tableParts(String table){int dot=table.lastIndexOf('.');return dot<0?new String[]{database,table}:new String[]{table.substring(0,dot),table.substring(dot+1)};}
 private String identifier(String s){if(s==null||!s.matches("[A-Za-z_][A-Za-z0-9_]{0,127}"))throw new IllegalArgumentException("Unsafe SQL identifier");return s;}
 private double estimate(QueryDefinition d,int rows,long elapsed){return Math.max(1,rows)*(1+(d.dimensions()==null?0:d.dimensions().size())+(d.filters()==null?0:d.filters().size()))+elapsed/1000.0;}
 private void audit(UUID tenant,String subject,String hash,long elapsed,int rows,double cost,boolean hit,String status){control.update("INSERT INTO sbase_bi_query_audit(tenant_id,subject,query_hash,elapsed_ms,returned_rows,estimated_cost,cache_hit,status) VALUES(?,?,?,?,?,?,?,?)",tenant,subject,hash,elapsed,rows,cost,hit,status);}
 private String write(Object x){try{return mapper.writeValueAsString(x);}catch(Exception e){throw new IllegalArgumentException(e);}}
 private String hash(String s){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
 private record CacheEntry(List<Map<String,Object>> rows,long expiresAt){}
 public record Metric(String column,String aggregate,String alias){}
 public record Filter(String column,String operator,Object value){}
 public record Sort(String column,String direction){}
 public record QueryDefinition(String table,List<String> dimensions,List<Metric> metrics,List<Filter> filters,List<Sort> orderBy,Integer limit){}
}
