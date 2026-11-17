package io.sbase.server.engines;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sbase.server.catalog.CatalogService;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.spark.sql.*;

import java.util.*;
import java.nio.file.Path;
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
        SqlNode parsed=SqlParser.create(CalciteSqlCompatibility.forParsing(sql)).parseQuery();
        if(!CalciteSqlCompatibility.isReadOnlyQuery(parsed))
            throw new IllegalArgumentException("Only read-only SELECT queries are supported");
        int rowLimit=Math.max(1,Math.min(maxRows,5000));
        if(sources==null||sources.isEmpty()||sources.size()>16)throw new IllegalArgumentException("Provide 1..16 source table definitions");
        Map<String,Future<List<Map<String,Object>>>> futures=new LinkedHashMap<>();
        for(var entry:sources.entrySet()){
            String table=entry.getKey();
            if(!table.matches("[A-Za-z_][A-Za-z0-9_]*"))throw new IllegalArgumentException("Invalid table alias: "+table);
            futures.put(table,scans.submit(()->scan(String.valueOf(entry.getValue().getOrDefault("type","postgresql")).toLowerCase(Locale.ROOT),entry.getValue(),rowLimit,sql,table)));
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
        List<Path> stagedFiles=new ArrayList<>();
        for(var entry:tables.entrySet()){
            Path staged=FederatedSpillStore.stage(entry.getValue());
            stagedFiles.add(staged);
            Dataset<Row> frame=requestSession.read().json(staged.toUri().toString());
            frame.createOrReplaceTempView(entry.getKey());
        }
        Dataset<Row> result=requestSession.sql(sql).limit(rowLimit);
        List<String> jsonResults=result.toJSON().collectAsList();
        List<Map<String,Object>> rows=new ArrayList<>();
        for(String row:jsonResults)rows.add(JSON.readValue(row,new TypeReference<>(){}));
        FederatedSpillStore.cleanup(stagedFiles);
        long inputRows=tables.values().stream().mapToLong(List::size).sum();
        Map<String,Object> response = new LinkedHashMap<>();
        response.put("status","SUCCEEDED");
        response.put("engine","calcite-validated-spark-sql");
        response.put("sourceCount",sources.size());
        response.put("sourceTables",tables.keySet());
        response.put("inputRows",inputRows);
        response.put("rows",rows);
        response.put("rowCount",rows.size());
        response.put("rowLimit",rowLimit);
        response.put("adaptiveExecution",true);
        response.put("costBasedJoinReorder",true);
        response.put("executionEngine","Spark SQL Catalyst with Adaptive Query Execution");
        return response;
    }

    private List<Map<String,Object>> scan(String type,Map<String,Object> spec,int limit,String sql,String table)throws Exception{
        return switch(type){
            case "postgresql"->{
                @SuppressWarnings("unchecked") List<Object> params=(List<Object>)spec.getOrDefault("parameters",List.of());
                String sourceQuery=spec.containsKey("query")?String.valueOf(spec.get("query")):FederatedPredicatePushdown.translate(sql,type,String.valueOf(spec.getOrDefault("table",table)),number(spec.get("limit"),limit)).orElseThrow(()->new IllegalArgumentException("Provide an explicit PostgreSQL source query when SQL cannot be safely pushed down"));
                Map<String,Object> result=catalogs.queryPostgres(sourceQuery,params,number(spec.get("limit"),limit));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");yield rows;
            }
            case "cassandra"->{
                String sourceQuery=spec.containsKey("query")?String.valueOf(spec.get("query")):FederatedPredicatePushdown.translate(sql,type,String.valueOf(spec.getOrDefault("table",table)),number(spec.get("limit"),limit)).orElseThrow(()->new IllegalArgumentException("Provide an explicit CQL source query when SQL cannot be safely pushed down"));
                Map<String,Object> result=catalogs.queryCassandra(sourceQuery,number(spec.get("limit"),limit));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");yield rows;
            }
            case "solr"->{
                String solrQuery=spec.containsKey("q")?String.valueOf(spec.get("q")):FederatedPredicatePushdown.translate(sql,type,null,number(spec.get("limit"),limit)).orElse("*:*");
                Map<String,Object> result=catalogs.querySolr(String.valueOf(spec.get("collection")),solrQuery,number(spec.get("limit"),limit));
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
