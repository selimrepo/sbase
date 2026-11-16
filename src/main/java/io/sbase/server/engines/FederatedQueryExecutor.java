package io.sbase.server.engines;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sbase.server.catalog.CatalogService;

import java.util.*;
import java.util.concurrent.*;

/**
 * Executes cross-source joins: source scans run concurrently, their bounded result
 * sets are materialized as Calcite tables, and Calcite executes the original SQL.
 */
public final class FederatedQueryExecutor implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final CatalogService catalogs;
    private final CalciteSqlEngine calcite;
    private final ExecutorService scans;

    public FederatedQueryExecutor(CatalogService catalogs, CalciteSqlEngine calcite, int parallelism) {
        this.catalogs=Objects.requireNonNull(catalogs);
        this.calcite=Objects.requireNonNull(calcite);
        this.scans=Executors.newFixedThreadPool(Math.max(2,Math.min(32,parallelism)));
    }

    public Map<String,Object> execute(String sql, Map<String,Map<String,Object>> sources, int maxRows) throws Exception {
        if(sql==null || sql.isBlank()) throw new IllegalArgumentException("sql is required");
        int rowLimit=Math.max(1,Math.min(maxRows,5000));
        if(sources==null || sources.isEmpty() || sources.size()>16)
            throw new IllegalArgumentException("Provide between 1 and 16 source table definitions");
        Map<String,Future<List<Map<String,Object>>>> futures=new LinkedHashMap<>();
        for(var entry:sources.entrySet()) {
            String table=entry.getKey();
            if(!table.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("Invalid table alias: "+table);
            Map<String,Object> spec=entry.getValue();
            String type=String.valueOf(spec.getOrDefault("type","postgresql")).toLowerCase(Locale.ROOT);
            futures.put(table,scans.submit(()->scan(type,spec,rowLimit)));
        }
        Map<String,List<Map<String,Object>>> tables=new LinkedHashMap<>();
        try {
            for(var entry:futures.entrySet()) {
                List<Map<String,Object>> rows=entry.getValue().get(45,TimeUnit.SECONDS);
                if(rows.isEmpty()) throw new IllegalStateException("Source returned no rows for table "+entry.getKey());
                tables.put(entry.getKey(),rows);
            }
        } catch(Exception error) {
            futures.values().forEach(f->f.cancel(true));
            throw error;
        }
        String result=calcite.executeJson(sql,JSON.writeValueAsString(tables));
        List<Map<String,Object>> rows=JSON.readValue(result,new TypeReference<>(){});
        return Map.of("status","SUCCEEDED","engine","calcite-enumerable-federated",
                "sourceCount",sources.size(),"sourceTables",tables.keySet(),
                "inputRows",tables.values().stream().mapToLong(List::size).sum(),
                "rows",rows,"rowCount",rows.size(),"rowLimit",rowLimit,
                "execution","parallel bounded source scans followed by Calcite local join/aggregation");
    }

    private List<Map<String,Object>> scan(String type,Map<String,Object> spec) throws Exception {
        return switch(type) {
            case "postgresql" -> {
                Object params=spec.getOrDefault("parameters",List.of());
                @SuppressWarnings("unchecked") List<Object> values=(List<Object>)params;
                Map<String,Object> result=catalogs.queryPostgres(String.valueOf(spec.get("query")),values,
                        number(spec.get("limit"),1000));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");
                yield rows;
            }
            case "cassandra" -> {
                Map<String,Object> result=catalogs.queryCassandra(String.valueOf(spec.get("query")),number(spec.get("limit"),1000));
                @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");
                yield rows;
            }
            case "solr" -> {
                Map<String,Object> result=catalogs.querySolr(String.valueOf(spec.get("collection")),
                        String.valueOf(spec.getOrDefault("q","*:*")),number(spec.get("limit"),1000));
                Object docs=result.get("documents");
                List<Map<String,Object>> rows=new ArrayList<>();
                if(docs instanceof Iterable<?> iterable) for(Object doc:iterable) {
                    @SuppressWarnings("unchecked") Map<String,Object> map=JSON.convertValue(doc,Map.class);
                    rows.add(map);
                }
                yield rows;
            }
            default -> throw new IllegalArgumentException("Unsupported federated source type: "+type);
        };
    }
    private static int number(Object value,int fallback) {
        return value instanceof Number n?Math.max(1,Math.min(5000,n.intValue())):fallback;
    }
    @Override public void close(){scans.shutdownNow();}
}
