package com.sbase.platform.feature;

import org.apache.spark.sql.*;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkFeatureEngineeringService {
    private final SparkSession spark;

    public SparkFeatureEngineeringService(@Lazy SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> transform(String inputUri, List<FeatureOperation> operations) {
        Dataset<Row> frame = spark.read().parquet(inputUri);
        for (FeatureOperation op : operations) {
            String output = identifier(op.output());
            String source = identifier(op.source());
            Column value = col(source);
            frame = switch (op.operation().toLowerCase(Locale.ROOT)) {
                case "cast_double" -> frame.withColumn(output, value.cast("double"));
                case "log1p" -> frame.withColumn(output, log1p(value.cast("double")));
                case "normalize_zscore" -> normalize(frame, output, value);
                case "bucket" -> {
                    if (op.buckets() < 2 || op.buckets() > 100) {
                        throw new IllegalArgumentException("Bucket count must be between 2 and 100");
                    }
                    yield frame.withColumn(output, when(value.isNull(), lit("missing"))
                            .otherwise(ntile(op.buckets())
                                    .over(org.apache.spark.sql.expressions.Window.orderBy(value))
                                    .cast("string")));
                }
                case "copy" -> frame.withColumn(output, value);
                default -> throw new IllegalArgumentException("Unsupported feature operation: " + op.operation());
            };
        }
        return frame;
    }

    private static Dataset<Row> normalize(Dataset<Row> frame, String output, Column source) {
        Column numeric = source.cast("double");
        Row stats = frame.select(avg(numeric), stddev_pop(numeric)).first();
        if (stats.isNullAt(0) || stats.isNullAt(1)) {
            return frame.withColumn(output, when(numeric.isNull(), lit(null).cast("double")).otherwise(lit(0.0)));
        }
        double mean = stats.getDouble(0);
        double deviation = stats.getDouble(1);
        if (deviation == 0.0) {
            return frame.withColumn(output, when(numeric.isNull(), lit(null).cast("double")).otherwise(lit(0.0)));
        }
        return frame.withColumn(output, (numeric.minus(mean)).divide(deviation));
    }

    public void materialize(String inputUri, String outputUri, List<FeatureOperation> operations) {
        transform(inputUri, operations).write().mode(SaveMode.Overwrite).parquet(outputUri);
    }

    private static String identifier(String value) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid feature column");
        }
        return value;
    }

    public record FeatureOperation(String operation, String source, String output, int buckets) { }
}
