package com.sbase.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class DorisConfig {
    @Bean
    DataSource dorisDataSource(DorisProperties properties) {
        DriverManagerDataSource source = new DriverManagerDataSource();
        source.setDriverClassName(properties.driverClassName());
        source.setUrl(properties.jdbcUrl());
        source.setUsername(properties.username());
        source.setPassword(properties.password());
        return source;
    }

    @Bean
    JdbcTemplate dorisJdbcTemplate(DataSource dorisDataSource) {
        return new JdbcTemplate(dorisDataSource);
    }
}
