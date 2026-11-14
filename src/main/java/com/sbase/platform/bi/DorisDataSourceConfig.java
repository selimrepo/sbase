package com.sbase.platform.bi;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariConfig;

import javax.sql.DataSource;

@Configuration
public class DorisDataSourceConfig {

    /**
     * The application's primary database. Keeping this bean explicit is
     * important: defining the Doris DataSource makes Spring Boot's default
     * DataSource auto-configuration back off. JPA and Flyway must use Postgres.
     */
    @Bean(name = "postgresPool", destroyMethod = "close")
    HikariDataSource primaryDataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String user,
            @Value("${spring.datasource.password}") String pass,
            @Value("${spring.datasource.hikari.maximum-pool-size:15}") int maxPoolSize,
            @Value("${spring.datasource.hikari.minimum-idle:2}") int minIdle) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setMinimumIdle(minIdle);
        ds.setPoolName("sbase-postgres");
        return ds;
    }

    @Bean(name = "dataSource", destroyMethod = "close")
    @Primary
    DataSource circuitBreakingPostgresDataSource(@Qualifier("postgresPool") DataSource pool,
            @Value("${sbase.resilience.postgres.failure-threshold:5}") int threshold,
            @Value("${sbase.resilience.postgres.open-seconds:15}") long openSeconds,
            @Value("${sbase.resilience.postgres.acquire-retries:2}") int retries,
            @Value("${sbase.resilience.postgres.retry-backoff-ms:75}") long backoff) {
        return new com.sbase.resilience.CircuitBreakingDataSource(pool, "postgres", threshold,
                java.time.Duration.ofSeconds(openSeconds), retries, backoff);
    }

    @Bean(name = "jdbcTemplate")
    @Primary
    JdbcTemplate jdbcTemplate(@Qualifier("dataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    /**
     * Compatibility aliases during the migration: legacy service names now
     * resolve to the single PostgreSQL pool. No secondary analytical database
     * or external JDBC endpoint is created.
     */
    @Bean(name = "dorisDataSource")
    DataSource legacyAnalyticsDataSource(@Qualifier("dataSource") DataSource postgres) {
        return postgres;
    }

    @Bean(name = "dorisJdbcTemplate")
    JdbcTemplate legacyAnalyticsJdbcTemplate(@Qualifier("dataSource") DataSource postgres) {
        return new JdbcTemplate(postgres);
    }

}
