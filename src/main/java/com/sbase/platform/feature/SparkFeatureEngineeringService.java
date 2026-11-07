package com.sbase.platform.feature;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;
import java.util.*;
import static org.apache.spark.sql.functions.*;

@Service
public class SparkFeatureEngineeringService {
 private final SparkSession spark;
 public SparkFeatureEngineeringService(SparkSession spark){this.spark=spark;}
 public Dataset<Row> transform(String inputUri,List<FeatureOperation> operations){
  Dataset<Row> frame=spark.read().parquet(inputUri);
  for(FeatureOperation op:operations){
   String out=identifier(op.output()),source=identifier(op.source());
   frame=switch(op.operation().toLowerCase(Locale.ROOT)){
    case "cast_double" -> frame.withColumn(out,col(source).cast("double"));
    case "log1p" -> frame.withColumn(out,log1p(col(source).cast("double")));
    case "normalize_zscore" -> {Column c=col(source).cast("double");double mean=frame.select(avg(c)).first().getDouble(0);double sd=frame.select(stddev_pop(c)).first().getDouble(0);yield frame.withColumn(out,sd==0?lit(0.0):(c.minus(mean).divide(sd)));}
    case "bucket" -> frame.withColumn(out,when(col(source).isNull(),lit("missing")).otherwise(ntile(op.buckets()).over(org.apache.spark.sql.expressions.Window.orderBy(col(source))).cast("string")));
    case "copy" -> frame.withColumn(out,col(source));
    default -> throw new IllegalArgumentException("Unsupported feature operation: "+op.operation());
   };
  }
  return frame;
 }
 public void materialize(String inputUri,String outputUri,List<FeatureOperation> operations){transform(inputUri,operations).write().mode(SaveMode.Overwrite).parquet(outputUri);}
 private static String identifier(String v){if(v==null||!v.matches("[A-Za-z_][A-Za-z0-9_]*"))throw new IllegalArgumentException("Invalid feature column");return v;}
 public record FeatureOperation(String operation,String source,String output,int buckets){}
}
