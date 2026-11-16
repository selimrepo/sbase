package io.sbase.server.engines;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.*;

/** Calcite-parser-backed, statistics-aware federated physical plan builder. */
public final class FederatedQueryPlanner {
    public Map<String,Object> plan(String sql, Map<String,Map<String,Object>> catalog) throws Exception {
        if(sql==null || sql.isBlank()) throw new IllegalArgumentException("query is required");
        SqlNode parsed=SqlParser.create(sql).parseQuery();
        if(parsed.getKind()!=SqlKind.SELECT && parsed.getKind()!=SqlKind.WITH)
            throw new IllegalArgumentException("Federated planner accepts read-only SELECT queries only");
        Set<String> identifiers=new LinkedHashSet<>();
        parsed.accept(new SqlBasicVisitor<Void>() {
            @Override public Void visit(SqlIdentifier id) {
                if(id.names.size()>0) identifiers.add(id.names.get(id.names.size()-1).toLowerCase(Locale.ROOT));
                return null;
            }
        });
        List<Map<String,Object>> scans=new ArrayList<>();
        for(var entry:catalog.entrySet()) {
            String table=entry.getKey().toLowerCase(Locale.ROOT);
            if(!identifiers.contains(table)) continue;
            Map<String,Object> stats=entry.getValue();
            String source=String.valueOf(stats.getOrDefault("source","postgresql"));
            long rows=number(stats.get("rows"),1_000_000L);
            long bytes=number(stats.get("bytes"),Math.max(1,rows)*128L);
            int partitions=(int)Math.max(1,Math.min(256,number(stats.get("partitions"),1)));
            double sourceWeight=switch(source.toLowerCase(Locale.ROOT)) {
                case "cassandra" -> 0.75;
                case "solr" -> 0.85;
                default -> 1.0;
            };
            scans.add(Map.of("table",entry.getKey(),"source",source,"estimatedRows",rows,
                    "estimatedBytes",bytes,"partitions",partitions,"scanCost",rows*sourceWeight));
        }
        if(scans.isEmpty()) throw new IllegalArgumentException("No referenced tables matched catalog statistics");
        scans.sort(Comparator.comparingLong(s -> ((Number)s.get("estimatedRows")).longValue()));
        Set<String> sources=new LinkedHashSet<>();
        long totalRows=0,totalBytes=0; int maxParallelism=1;
        for(Map<String,Object> scan:scans) {
            sources.add(String.valueOf(scan.get("source")));
            totalRows=saturatingAdd(totalRows,((Number)scan.get("estimatedRows")).longValue());
            totalBytes=saturatingAdd(totalBytes,((Number)scan.get("estimatedBytes")).longValue());
            maxParallelism=Math.max(maxParallelism,((Number)scan.get("partitions")).intValue());
        }
        boolean crossSource=sources.size()>1;
        boolean filterPushdown=sql.toLowerCase(Locale.ROOT).contains(" where ");
        boolean projectionPushdown=sql.toLowerCase(Locale.ROOT).contains("select ");
        double exchangeCost=crossSource?Math.max(1,totalBytes/1024.0):0.0;
        double scanCost=scans.stream().mapToDouble(s->((Number)s.get("scanCost")).doubleValue()).sum();
        double estimatedCost=scanCost+exchangeCost+(crossSource?totalRows*0.15:0);
        Map<String,Object> joinStrategy=crossSource
                ? Map.of("type","BROADCAST_SMALL_SIDE_OR_HASH_SHUFFLE","buildSide","smallest-estimated-input",
                         "exchange","bounded-partitioned-exchange","estimatedExchangeBytes",totalBytes)
                : Map.of("type","SOURCE_LOCAL_PUSHDOWN","exchange","none");
        Map<String,Object> plan = new LinkedHashMap<>();
        plan.put("engine","apache-calcite-federated-planner");
        plan.put("sql",sql);
        plan.put("scans",scans);
        plan.put("scanOrder","ascending-estimated-cardinality");
        plan.put("sources",sources);
        plan.put("crossSourceJoin",crossSource);
        plan.put("filterPushdownCandidate",filterPushdown);
        plan.put("projectionPushdownCandidate",projectionPushdown);
        plan.put("joinStrategy",joinStrategy);
        plan.put("parallelism",maxParallelism);
        plan.put("estimatedRows",totalRows);
        plan.put("estimatedBytes",totalBytes);
        plan.put("estimatedCost",estimatedCost);
        plan.put("notes",List.of("Estimates depend on supplied catalog statistics",
                "This endpoint returns a physical plan; execution and adaptive re-optimization are separate"));
        return plan;
    }
    private static long number(Object value,long fallback) {
        return value instanceof Number n?Math.max(0,n.longValue()):fallback;
    }
    private static long saturatingAdd(long a,long b) {
        try{return Math.addExact(a,b);}catch(ArithmeticException e){return Long.MAX_VALUE;}
    }
}
