package com.sbase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "sbase.doris")
public record DorisProperties(String jdbcUrl, String username, String password, String driverClassName) { }
