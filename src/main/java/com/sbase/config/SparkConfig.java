package com.sbase.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Bean(destroyMethod = "stop")
    @Lazy
    SparkSession sparkSession(SparkProperties properties) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(properties.appName())
                .master(properties.master())
                .config("spark.sql.shuffle.partitions", properties.shufflePartitions())
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",
                    System.getenv().getOrDefault("SPARK_ADVISORY_PARTITION_BYTES", "67108864"))
                .config("spark.default.parallelism",
                    System.getenv().getOrDefault("SPARK_DEFAULT_PARALLELISM",
                        Integer.toString(properties.shufflePartitions())))
                .config("spark.sql.files.maxPartitionBytes",
                    System.getenv().getOrDefault("SPARK_MAX_FILE_PARTITION_BYTES", "134217728"))
                // Spark 3.5 bundles a javax.servlet-based Jetty UI. The host app is Jakarta/Spring Boot 3.
                // The UI is not needed for the embedded driver and otherwise causes Jersey servlet startup failure.
                .config("spark.ui.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.driver.host", System.getenv().getOrDefault("SPARK_DRIVER_HOST", "127.0.0.1"))
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.port", "7078")
                .config("spark.blockManager.port", "7079");
        String extraJars = System.getenv("SPARK_EXTRA_JARS");
        if (extraJars != null && !extraJars.isBlank()) {
            builder.config("spark.jars", extraJars);
        }
        return builder.getOrCreate();
    }
}
