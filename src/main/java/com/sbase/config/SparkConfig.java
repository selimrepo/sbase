package com.sbase.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Bean(destroyMethod = "stop")
    SparkSession sparkSession(SparkProperties properties) {
        return SparkSession.builder()
                .appName(properties.appName())
                .master(properties.master())
                .config("spark.sql.shuffle.partitions", properties.shufflePartitions())
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.driver.host", "0.0.0.0")
                .getOrCreate();
    }
}
