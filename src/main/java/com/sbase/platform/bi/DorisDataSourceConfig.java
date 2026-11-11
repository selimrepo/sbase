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
    @Bean(name = "dataSource")
    @Primary
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

    @Bean(name = "jdbcTemplate")
    @Primary
    JdbcTemplate jdbcTemplate(@Qualifier("dataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "dorisDataSource", destroyMethod = "close")
    HikariDataSource dorisDataSource(
            @Value("${sbase.doris.jdbc-url}") String url,
            @Value("${sbase.doris.username}") String user,
            @Value("${sbase.doris.password}") String pass,
            @Value("${sbase.doris.driver-class-name}") String driver,
            @Value("${sbase.doris.pool.maximum-size:12}") int maxPool,
            @Value("${sbase.doris.pool.minimum-idle:1}") int minIdle,
            @Value("${sbase.doris.pool.connection-timeout-ms:3000}") long connectionTimeout,
            @Value("${sbase.doris.pool.validation-timeout-ms:1000}") long validationTimeout,
            @Value("${sbase.doris.pool.max-lifetime-ms:1500000}") long maxLifetime) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url); config.setUsername(user); config.setPassword(pass);
        config.setDriverClassName(driver);
        config.setMaximumPoolSize(Math.max(1, Math.min(64, maxPool)));
        config.setMinimumIdle(Math.max(0, Math.min(minIdle, config.getMaximumPoolSize())));
        config.setConnectionTimeout(Math.max(1000, connectionTimeout));
        config.setValidationTimeout(Math.max(250, validationTimeout));
        config.setMaxLifetime(Math.max(30000, maxLifetime));
        config.setPoolName("sbase-doris"); config.setInitializationFailTimeout(-1);
        config.setConnectionTestQuery("SELECT 1");
        return new HikariDataSource(config);
    }
    @Bean(name = "dorisJdbcTemplate")
    JdbcTemplate dorisJdbcTemplate(@Qualifier("dorisDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
