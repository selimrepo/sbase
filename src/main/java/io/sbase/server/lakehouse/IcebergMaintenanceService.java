package io.sbase.server.lakehouse;

import org.apache.spark.sql.SparkSession;
import java.util.Objects;

/** Executes Iceberg maintenance procedures through the configured Spark catalog. */
public final class IcebergMaintenanceService {
    private final SparkSession spark;
    public IcebergMaintenanceService(SparkSession spark) { this.spark=Objects.requireNonNull(spark); }

    public void compact(String catalog,String namespace,String table,int targetFileSizeMb) {
        if(targetFileSizeMb<32 || targetFileSizeMb>1024) throw new IllegalArgumentException("target file size must be 32..1024 MiB");
        String id=tableId(catalog,namespace,table);
        spark.sql("CALL "+catalog+".system.rewrite_data_files(table => '"+escape(id)+"', options => map('target-file-size-bytes', '"+(targetFileSizeMb*1024L*1024L)+"'))");
    }

    public void rewriteManifests(String catalog,String namespace,String table) {
        spark.sql("CALL "+catalog+".system.rewrite_manifests(table => '"+escape(tableId(catalog,namespace,table))+"')");
    }

    public void expireSnapshots(String catalog,String namespace,String table,int retainLast,int olderThanDays) {
        if(retainLast<1 || retainLast>1000 || olderThanDays<1) throw new IllegalArgumentException("invalid snapshot retention");
        long cutoff=System.currentTimeMillis()-olderThanDays*86400000L;
        spark.sql("CALL "+catalog+".system.expire_snapshots(table => '"+escape(tableId(catalog,namespace,table))+
            "', older_than => TIMESTAMP '"+new java.sql.Timestamp(cutoff)+"', retain_last => "+retainLast+")");
    }

    public void removeOrphanFiles(String catalog,String namespace,String table,int olderThanHours) {
        if(olderThanHours<24) throw new IllegalArgumentException("orphan file age must be at least 24 hours");
        long cutoff=System.currentTimeMillis()-olderThanHours*3600000L;
        spark.sql("CALL "+catalog+".system.remove_orphan_files(table => '"+escape(tableId(catalog,namespace,table))+
            "', older_than => TIMESTAMP '"+new java.sql.Timestamp(cutoff)+"')");
    }

    private static String tableId(String catalog,String namespace,String table) {
        return identifier(catalog)+"."+identifier(namespace)+"."+identifier(table);
    }
    private static String identifier(String value) {
        if(value==null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("Unsafe Iceberg identifier");
        return value;
    }
    private static String escape(String value) { return value.replace("'","''"); }
}
