package com.sbase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "sbase.postgres")
public record PostgresProperties(String jdbcUrl, String username, String password,
                                 String driverClassName, String database, String allowedTables) { }
