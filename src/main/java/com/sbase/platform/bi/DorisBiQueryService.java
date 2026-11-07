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
 private final Set<String> allowed;
 public DorisBiQueryService(@Value("${sbase.doris.allowed-tables:}") String tables,@Qualifier("dorisJdbcTemplate") JdbcTemplate doris){
  this.doris=doris;this.allowed=new HashSet<>();for(String t:tables.split(","))if(!t.isBlank())allowed.add(t.trim().toLowerCase(Locale.ROOT));
 }
 public List<Map<String,Object>> query(String sql){
  if(sql==null||sql.length()>12000||sql.contains(";")||sql.contains("--")||sql.contains("/*"))throw new IllegalArgumentException("Only one bounded SELECT statement is allowed");
  Matcher m=SAFE.matcher(sql);if(!m.matches()||!allowed.contains(m.group(1).toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Query must select from an allow-listed Doris table");
  String lower=sql.toLowerCase(Locale.ROOT);if(!lower.matches("(?s).*\\blimit\\s+[0-9]+\\s*$"))sql=sql+" LIMIT 1000";
  return doris.queryForList(sql);
 }
}
