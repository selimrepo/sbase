package com.sbase.doris;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class DorisDatasetWriter {
    private final DorisClient doris;

    public DorisDatasetWriter(DorisClient doris) { this.doris = doris; }

    /**
     * Appends rows for the authenticated tenant. The tenant column is injected
     * by the platform rather than trusted from an incoming dataset.
     */
    public void append(Dataset<Row> dataset, String table, UUID tenantId) {
        if (tenantId == null) throw new IllegalArgumentException("Tenant ID is required for Doris writes");
        Dataset<Row> tenantScoped = dataset.withColumn("tenant_id", functions.lit(tenantId.toString()));
        write(tenantScoped, table);
    }

    /**
     * Compatibility path for already tenant-scoped datasets. New ingestion
     * code should call append(dataset, table, tenantId) so tenant ownership
     * is assigned by the platform.
     */
    public void append(Dataset<Row> dataset, String table) {
        if (!java.util.Arrays.asList(dataset.columns()).contains("tenant_id")) {
            throw new IllegalArgumentException("Doris BI datasets require tenant_id; use append(dataset, table, tenantId)");
        }
        write(dataset, table);
    }

    private void write(Dataset<Row> dataset, String table) {
        if (table == null || !table.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?")) {
            throw new IllegalArgumentException("Invalid Doris table identifier");
        }
        dataset.write()
                .format("jdbc")
                .option("url", doris.jdbcUrl())
                .option("dbtable", table)
                .option("user", doris.username())
                .option("password", doris.password())
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("batchsize", "5000")
                .mode("append")
                .save();
    }
}
