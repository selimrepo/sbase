package com.sbase.doris;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

@Service
public class DorisDatasetWriter {
    private final DorisClient doris;

    public DorisDatasetWriter(DorisClient doris) { this.doris = doris; }

    public void append(Dataset<Row> dataset, String table) {
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
