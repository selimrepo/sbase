package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.regex.*;

@Service
public class DorisBiQueryService {
 private final JdbcTemplate doris;
 private final DorisTenantTablePolicy tablePolicy;
 private static final Pattern SAFE=Pattern.compile("(?is)^\\s*select\\s+.+?\\s+from\\s+([A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?)\\b.*$");
 private static final Pattern LIMIT=Pattern.compile("(?is)\\blimit\\s+(\\d+)\\s*$");
 private static final Pattern UNSUPPORTED=Pattern.compile("(?is)\\b(union|join|with|into|outfile|load_file|sleep|benchmark|insert|update|delete|drop|alter|create|grant|revoke|set|use|call|offset)\\b");
 private static final Pattern CLAUSE=Pattern.compile("(?is)\\s+(group\\s+by|order\\s+by|limit)\\b");
 public DorisBiQueryService(@Qualifier("dorisJdbcTemplate") JdbcTemplate doris,DorisTenantTablePolicy tablePolicy){
  this.doris=doris;this.tablePolicy=tablePolicy;
 }

 public List<Map<String,Object>> query(UUID tenantId,String sql){
  if(tenantId==null)throw new IllegalArgumentException("Tenant ID is required for PostgreSQL queries");
  if(sql==null||sql.isBlank()||sql.length()>12000||sql.contains(";")||sql.contains("--")||sql.contains("/*"))
   throw new IllegalArgumentException("Only one bounded SELECT statement is allowed");
  String normalized=sql.strip();
  if(UNSUPPORTED.matcher(normalized).find()||countSelects(normalized)!=1||Pattern.compile("(?is)\\bfrom\\s+[A-Za-z_][A-Za-z0-9_.]*\\s*,").matcher(normalized).find()
      ||Pattern.compile("(?is)\\btenant_id\\b").matcher(normalized).find())
   throw new IllegalArgumentException("Only a single-table SELECT without joins, subqueries or tenant overrides is allowed");
  Matcher table=SAFE.matcher(normalized);
  if(!table.matches())throw new IllegalArgumentException("Query must select from one allow-listed PostgreSQL table");
  String canonical=tablePolicy.requireTenantScoped(table.group(1));
  normalized=normalized.replaceFirst("(?i)(\\bfrom\\s+)"+Pattern.quote(table.group(1))+"\\b","$1"+canonical);
  Matcher limit=LIMIT.matcher(normalized);
  if(limit.find()){
   long requested;
   try{requested=Long.parseLong(limit.group(1));}catch(NumberFormatException ex){throw new IllegalArgumentException("Invalid query limit",ex);}
   if(requested<1||requested>10000)throw new IllegalArgumentException("Query limit must be between 1 and 10000");
  }else{
   if(Pattern.compile("(?is)\\blimit\\b").matcher(normalized).find())throw new IllegalArgumentException("Invalid query limit");
   normalized += " LIMIT 1000";
  }
  Matcher clause=CLAUSE.matcher(normalized);String suffix="";
  if(clause.find()){suffix=normalized.substring(clause.start());normalized=normalized.substring(0,clause.start());}
  Matcher where=Pattern.compile("(?is)\\bwhere\\b").matcher(normalized);
  if(where.find())normalized=normalized.substring(0,where.start())+"WHERE ("+normalized.substring(where.end()).strip()+") AND tenant_id = ?";
  else normalized += " WHERE tenant_id = ?";
  return doris.queryForList(normalized+suffix,tenantId.toString());
 }
 @Deprecated public List<Map<String,Object>> query(String sql){
  throw new IllegalStateException("Unscoped PostgreSQL queries are disabled; supply the authenticated tenant ID");
 }
 private static int countSelects(String sql){Matcher m=Pattern.compile("(?i)\\bselect\\b").matcher(sql);int n=0;while(m.find())n++;return n;}
}
