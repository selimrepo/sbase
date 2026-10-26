package com.sbase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "sbase.spark")
public record SparkProperties(String master, String appName, int shufflePartitions) { }
