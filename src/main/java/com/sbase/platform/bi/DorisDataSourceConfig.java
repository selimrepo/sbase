package com.sbase.platform.bi;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

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
            @Value("${sbase.doris.pool.maximum-pool-size:12}") int maxPoolSize,
            @Value("${sbase.doris.pool.minimum-idle:2}") int minIdle,
            @Value("${sbase.doris.pool.connection-timeout:5000}") long connectionTimeout,
            @Value("${sbase.doris.pool.validation-timeout:3000}") long validationTimeout,
            @Value("${sbase.doris.pool.max-lifetime:1800000}") long maxLifetime) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setDriverClassName(driver);
        ds.setPoolName("sbase-doris");
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setMinimumIdle(minIdle);
        ds.setConnectionTimeout(connectionTimeout);
        ds.setValidationTimeout(validationTimeout);
        ds.setMaxLifetime(maxLifetime);
        ds.setInitializationFailTimeout(-1);
        return ds;
    }

    @Bean(name = "dorisJdbcTemplate")
    JdbcTemplate dorisJdbcTemplate(@Qualifier("dorisDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
