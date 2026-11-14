package com.sbase.doris;

import com.sbase.platform.bi.DorisTenantTablePolicy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;
import java.util.Arrays;
import java.util.UUID;

@Service
public class DorisDatasetWriter {
    private final DorisClient doris;
    private final DorisTenantTablePolicy tablePolicy;

    public DorisDatasetWriter(DorisClient doris,DorisTenantTablePolicy tablePolicy) {
        this.doris=doris;this.tablePolicy=tablePolicy;
    }

    /** Tenant ownership is checked before any data is sent to Doris. */
    public void append(Dataset<Row> dataset,String table,UUID tenantId) {
        if(dataset==null || tenantId==null) throw new IllegalArgumentException("Dataset and tenant ID are required");
        String governedTable=tablePolicy.requireTenantScoped(table);
        if(!Arrays.asList(dataset.columns()).contains("tenant_id"))
            throw new org.springframework.security.access.AccessDeniedException("PostgreSQL write requires an explicit tenant_id column");
        if(dataset.filter(functions.col("tenant_id").isNull()
                .or(functions.col("tenant_id").notEqual(functions.lit(tenantId.toString())))).limit(1).count()>0)
            throw new org.springframework.security.access.AccessDeniedException("PostgreSQL write contains rows outside the authenticated tenant");
        write(dataset,governedTable);
    }

    /** There is deliberately no implicit-tenant compatibility write path. */
    @Deprecated
    public void append(Dataset<Row> dataset,String table) {
        throw new IllegalStateException("Unscoped PostgreSQL writes are disabled; supply the authenticated tenant ID");
    }

    private void write(Dataset<Row> dataset,String table) {
        dataset.write().format("jdbc").option("url",doris.jdbcUrl()).option("dbtable",table)
            .option("user",doris.username()).option("password",doris.password())
            .option("driver","org.postgresql.Driver").option("batchsize","5000")
            .mode("append").save();
    }
}
