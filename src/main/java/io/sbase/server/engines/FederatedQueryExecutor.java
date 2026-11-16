package io.sbase.server.engines;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sbase.server.catalog.CatalogService;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Parallel source extraction followed by Spark SQL execution. Each request receives
 * an isolated Spark SQL session; Spark can distribute joins/shuffles to its configured
 * cluster while Calcite validates the SQL grammar before any connector is contacted.
 */
public final class FederatedQueryExecutor implements AutoCloseable {
    private static final ObjectMapper JSON=new ObjectMapper();
    private final CatalogService catalogs;
    private final SparkSession spark;
    private final ExecutorService scans;

    public FederatedQueryExecutor(CatalogService catalogs,SparkSession spark,int parallelism){
        this.catalogs=Objects.requireNonNull(catalogs);this.spark=Objects.requireNonNull(spark);
        this.scans=Executors.newFixedThreadPool(Math.max(2,Math.min(32,parallelism)));
    }

    public Map<String,Object> execute(String sql,Map<String,Map<String,Object>> sources,int maxRows)throws Exception{
        if(sql==null||sql.isBlank())throw new IllegalArgumentException("sql is required");
        SqlNode parsed=SqlParser.create(sql).parseQuery();
        if(parsed.getKind()!=SqlKind.SELECT&&parsed.getKind()!=SqlKind.WITH)
            throw new IllegalArgumentException("Only read-only SELECT queries are supported");
        int rowLimit=Math.max(1,Math.min(maxRows,5000));
        if(sources==null||sources.isEmpty()||sources.size()>16)throw new IllegalArgumentException("Provide 1..16 source table definitions");
        Map<String,Future<List<Map<String,Object>>>> futures=new LinkedHashMap<>();
        for(var entry:sources.entrySet()){
            String table=entry.getKey();
            if(!table.matches("[A-Za-z_][A-Za-z0-9_]*"))throw new IllegalArgumentException("Invalid table alias: "+table);
            futures.put(table,scans.submit(()->scan(String.valueOf(entry.getValue().getOrDefault("type","postgresql")).toLowerCase(Locale.ROOT),entry.getValue(),rowLimit)));
        }
        Map<String,List<Map<String,Object>>> tables=new LinkedHashMap<>();
        try{
            for(var entry:futures.entrySet()){
                List<Map<String,Object>> rows=entry.getValue().get(45,TimeUnit.SECONDS);
                if(rows.isEmpty())throw new IllegalStateException("Source returned no rows for table "+entry.getKey());
                tables.put(entry.getKey(),rows);
            }
        }catch(Exception error){futures.values().forEach(f->f.cancel(true));throw error;}

        SparkSession requestSession=spark.newSession();
        requestSession.conf().set("spark.sql.adaptive.enabled","true");
        requestSession.conf().set("spark.sql.adaptive.coalescePartitions.enabled","true");
        requestSession.conf().set("spark.sql.cbo.enabled","true");
        requestSession.conf().set("spark.sql.cbo.joinReorder.enabled","true");
        for(var entry:tables.entrySet()){
            List<String> jsonRows=entry.getValue().stream().map(JSON::valueToTree).map(Object::toString).toList();
            Dataset<Row> frame=requestSession.read().json(requestSession.createDataset(jsonRows,Encoders.STRING()));
            frame.createOrReplaceTempView(entry.getKey());
        }
        Dataset<Row> result=requestSession.sql(sql).limit(rowLimit);
        List<String> jsonResults=result.toJSON().collectAsList();
        List<Map<String,Object>> rows=new ArrayList<>();
        for(String row:jsonResults)rows.add(JSON.readValue(row,new TypeReference<>(){}));
        long inputRows=tables.values().stream().mapToLong(List::size).sum();
        return Map.of("status","SUCCEEDED","engine","calcite-validated-spark-sql",
                "sourceCount",sources.size(),"sourceTables",tables.keySet(),"inputRows",inputRows,
                "rows",rows,"rowCount",rows.size(),"rowLimit",rowLimit,
                "adaptiveExecution",true,"costBasedJoinReorder",true,"executionEngine","Spark SQL Catalyst with Adaptive Query Execution");
    }

    private List<Map<String,Object>> scan(String type,Map<String,Object> spec,int limit)throws Exception{
        return switch(type){
            case "postgresql"->{
                @SuppressWarnings("unchecked") List<Object> params=(List<Object>)spec.getOrDefault("parameters",List.of());
                Map<String,Object> result=catalogs.queryPostgres(String.valueOf(spec.get("query")),params,number(spec.get("limit"),limit));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");yield rows;
            }
            case "cassandra"->{
                Map<String,Object> result=catalogs.queryCassandra(String.valueOf(spec.get("query")),number(spec.get("limit"),limit));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");yield rows;
            }
            case "solr"->{
                Map<String,Object> result=catalogs.querySolr(String.valueOf(spec.get("collection")),String.valueOf(spec.getOrDefault("q","*:*")),number(spec.get("limit"),limit));
                Object docs=result.get("documents");List<Map<String,Object>> rows=new ArrayList<>();
                if(docs instanceof Iterable<?> iterable)for(Object doc:iterable){
                    @SuppressWarnings("unchecked") Map<String,Object> map=JSON.convertValue(doc,Map.class);rows.add(map);
                }yield rows;
            }
            default->throw new IllegalArgumentException("Unsupported federated source type: "+type);
        };
    }
    private static int number(Object value,int fallback){return value instanceof Number n?Math.max(1,Math.min(5000,n.intValue())):fallback;}
    @Override public void close(){scans.shutdownNow();}
}
