package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.regex.*;

@Service
public class DorisBiQueryService {
 private final JdbcTemplate doris;
 private static final Pattern SAFE=Pattern.compile("(?is)^\\s*select\\s+.+?\\s+from\\s+([A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?)\\b.*$");
 private static final Pattern LIMIT=Pattern.compile("(?is)\\blimit\\s+(\\d+)\\s*$");
 private static final Pattern UNSUPPORTED=Pattern.compile("(?is)\\b(union|join|with|into|outfile|load_file|sleep|benchmark|insert|update|delete|drop|alter|create|grant|revoke|set|use|call|offset)\\b");
 private final Set<String> allowed;
 public DorisBiQueryService(@Value("${sbase.doris.allowed-tables:}") String tables,@Qualifier("dorisJdbcTemplate") JdbcTemplate doris){
  this.doris=doris;this.allowed=new HashSet<>();for(String t:tables.split(","))if(!t.isBlank())allowed.add(t.trim().toLowerCase(Locale.ROOT));
 }
 public List<Map<String,Object>> query(String sql){
  if(sql==null||sql.isBlank()||sql.length()>12000||sql.contains(";")||sql.contains("--")||sql.contains("/*"))
   throw new IllegalArgumentException("Only one bounded SELECT statement is allowed");
  String normalized=sql.strip();
  if(UNSUPPORTED.matcher(normalized).find()||countSelects(normalized)!=1||Pattern.compile("(?is)\\bfrom\\s+[A-Za-z_][A-Za-z0-9_.]*\\s*,").matcher(normalized).find())
   throw new IllegalArgumentException("Only a single-table SELECT without joins or subqueries is allowed");
  Matcher table=SAFE.matcher(normalized);
  if(!table.matches()||!allowed.contains(table.group(1).toLowerCase(Locale.ROOT)))
   throw new IllegalArgumentException("Query must select from an allow-listed Doris table");
  Matcher limit=LIMIT.matcher(normalized);
  if(limit.find()){
   long requested;
   try{requested=Long.parseLong(limit.group(1));}catch(NumberFormatException ex){throw new IllegalArgumentException("Invalid query limit",ex);}
   if(requested<1||requested>10000)throw new IllegalArgumentException("Query limit must be between 1 and 10000");
  }else{
   if(Pattern.compile("(?is)\\blimit\\b").matcher(normalized).find())throw new IllegalArgumentException("Invalid query limit");
   normalized += " LIMIT 1000";
  }
  return doris.queryForList(normalized);
 }
 private static int countSelects(String sql){
  Matcher matcher=Pattern.compile("(?i)\\bselect\\b").matcher(sql);int count=0;while(matcher.find())count++;return count;
 }
}
