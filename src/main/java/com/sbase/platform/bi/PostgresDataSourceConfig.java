package com.sbase.platform.bi;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * One physical PostgreSQL pool for application data, BI, ingestion and Spark
 * coordination. The legacy Postgres bean names are temporary aliases so the
 * migration can be performed without creating a second connection pool.
 */
@Configuration
public class PostgresDataSourceConfig {
    @Bean(name = "postgresPool", destroyMethod = "close")
    HikariDataSource postgresPool(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.hikari.maximum-pool-size:20}") int maximumPoolSize,
            @Value("${spring.datasource.hikari.minimum-idle:2}") int minimumIdle) {
        HikariDataSource pool = new HikariDataSource();
        pool.setJdbcUrl(url);
        pool.setUsername(username);
        pool.setPassword(password);
        pool.setDriverClassName("org.postgresql.Driver");
        pool.setMaximumPoolSize(Math.max(2, Math.min(64, maximumPoolSize)));
        pool.setMinimumIdle(Math.max(0, Math.min(minimumIdle, pool.getMaximumPoolSize())));
        pool.setPoolName("sbase-postgres");
        pool.setConnectionTestQuery("SELECT 1");
        return pool;
    }

    @Bean(name = {"dataSource", "postgresDataSource", "postgresDataSource"})
    @Primary
    DataSource dataSource(@Qualifier("postgresPool") DataSource pool,
            @Value("${sbase.resilience.postgres.failure-threshold:5}") int threshold,
            @Value("${sbase.resilience.postgres.open-seconds:15}") long openSeconds,
            @Value("${sbase.resilience.postgres.acquire-retries:2}") int retries,
            @Value("${sbase.resilience.postgres.retry-backoff-ms:75}") long backoff) {
        return new com.sbase.resilience.CircuitBreakingDataSource(pool, "postgres",
                threshold, java.time.Duration.ofSeconds(openSeconds), retries, backoff);
    }

    @Bean(name = {"jdbcTemplate", "postgresJdbcTemplate", "postgresJdbcTemplate"})
    @Primary
    JdbcTemplate jdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
