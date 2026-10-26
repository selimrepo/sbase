package com.sbase.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Bean(destroyMethod = "stop")
    SparkSession sparkSession(SparkProperties properties) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(properties.appName())
                .master(properties.master())
                .config("spark.sql.shuffle.partitions", properties.shufflePartitions())
                .config("spark.sql.adaptive.enabled", "true")
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
