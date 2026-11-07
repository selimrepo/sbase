package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;
import java.sql.*;
import java.util.*;

@Component
public class JdbcIngestionConnector implements IngestionConnector {
 public String type(){return "jdbc";}
 public ConnectorResult ingest(ConnectorRequest request){
  Map<String,String> o=request.options();
  String url=required(o,"jdbcUrl"),user=o.getOrDefault("username",""),password=o.getOrDefault("password","");
  String query=required(o,"query"),target=required(o,"targetTable");
  if(!query.stripLeading().toLowerCase(Locale.ROOT).startsWith("select")) throw new IllegalArgumentException("Only SELECT source queries are allowed");
  long read=0,written=0;
  try(Connection source=DriverManager.getConnection(url,user,password);
      Statement st=source.createStatement();ResultSet rs=st.executeQuery(query)){
   ResultSetMetaData md=rs.getMetaData();int n=md.getColumnCount();
   String placeholders=String.join(",",Collections.nCopies(n,"?"));
   StringJoiner columnNames=new StringJoiner(",");
   for(int i=1;i<=n;i++) columnNames.add(quote(md,i));
   String columns=columnNames.toString();
   String insert="INSERT INTO "+safeIdentifier(target)+" ("+columns+") VALUES ("+placeholders+")";
   try(Connection dest=DriverManager.getConnection(required(o,"targetJdbcUrl"),o.getOrDefault("targetUsername",""),o.getOrDefault("targetPassword",""));
       PreparedStatement ps=dest.prepareStatement(insert)){
    dest.setAutoCommit(false);
    while(rs.next()){for(int i=1;i<=n;i++)ps.setObject(i,rs.getObject(i));ps.addBatch();read++;if(read%1000==0){int[] counts=ps.executeBatch();written+=Arrays.stream(counts).filter(x->x>0||x==Statement.SUCCESS_NO_INFO).count();}}
    int[] counts=ps.executeBatch();written+=Arrays.stream(counts).filter(x->x>0||x==Statement.SUCCESS_NO_INFO).count();dest.commit();
   }
  }catch(SQLException e){throw new IllegalStateException("JDBC ingestion failed",e);}
  return new ConnectorResult(read,written,"JDBC batch ingestion completed");
 }
 private static String required(Map<String,String> o,String k){String v=o.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
 private static String safeIdentifier(String s){if(!s.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?"))throw new IllegalArgumentException("Unsafe SQL identifier");return s;}
 private static String quote(ResultSetMetaData md,int i)throws SQLException{return safeIdentifier(md.getColumnLabel(i));}
}
