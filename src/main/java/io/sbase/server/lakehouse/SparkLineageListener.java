package io.sbase.server.lakehouse;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Observes successful Spark SQL logical plans and persists table lineage. */
public final class SparkLineageListener implements QueryExecutionListener {
    private static final Pattern RELATION = Pattern.compile(
        "(?i)(?:UnresolvedRelation|DataSourceV2Relation|HiveTableRelation|LogicalRelation)\\s*\\[([^\\]]+)\\]");
    private static final Pattern TARGET = Pattern.compile(
        "(?i)(?:AppendData|OverwriteByExpression|OverwritePartitionsDynamic|InsertIntoStatement|InsertIntoHadoopFsRelationCommand).*?\\[([^\\]]+)\\]",
        Pattern.DOTALL);

    private final LakehouseControlPlane controlPlane;
    private final Supplier<UUID> workspace;
    private final Supplier<String> principal;

    public SparkLineageListener(LakehouseControlPlane controlPlane, Supplier<UUID> workspace, Supplier<String> principal) {
        this.controlPlane=Objects.requireNonNull(controlPlane);
        this.workspace=Objects.requireNonNull(workspace);
        this.principal=Objects.requireNonNull(principal);
    }

    public static SparkLineageListener install(SparkSession spark, LakehouseControlPlane controlPlane,
                                                Supplier<UUID> workspace, Supplier<String> principal) {
        SparkLineageListener listener=new SparkLineageListener(controlPlane,workspace,principal);
        spark.listenerManager().register(listener);
        return listener;
    }

    @Override public void onSuccess(String functionName, QueryExecution query, long durationNs) {
        String plan=query.analyzed().toString();
        Set<String> relations=new LinkedHashSet<>();
        Matcher matcher=RELATION.matcher(plan);
        while(matcher.find()) {
            String name=normalize(matcher.group(1));
            if(!name.isBlank()) relations.add(name);
        }
        Matcher targetMatcher=TARGET.matcher(plan);
        if(!targetMatcher.find()) return;
        TableId target=TableId.parse(normalize(targetMatcher.group(1)));
        UUID workspaceId=workspace.get();
        String actor=Objects.toString(principal.get(),"spark");
        for(String relation:relations) {
            TableId source=TableId.parse(relation);
            if(source.equals(target)) continue;
            String details="{\"engine\":\"SPARK\",\"function\":\""+escape(functionName)+"\",\"durationNs\":"+durationNs+"}";
            controlPlane.recordLineage(workspaceId,source.catalog,source.namespace,source.table,
                target.catalog,target.namespace,target.table,"DERIVED_FROM",null,null,actor,details);
        }
    }

    @Override public void onFailure(String functionName, QueryExecution query, Exception exception) {
        // Failed statements do not produce committed lineage edges.
    }

    private static String normalize(String raw) { return raw.replace("`","").replace("'","").trim(); }
    private static String escape(String value) { return Objects.toString(value,"").replace("\\","\\\\").replace("\"","\\\""); }

    private record TableId(String catalog,String namespace,String table) {
        static TableId parse(String value) {
            String[] p=value.split("\\.");
            if(p.length>=3) return new TableId(p[p.length-3],p[p.length-2],p[p.length-1]);
            if(p.length==2) return new TableId("sbase",p[0],p[1]);
            return new TableId("sbase","default",p[0]);
        }
    }
}
