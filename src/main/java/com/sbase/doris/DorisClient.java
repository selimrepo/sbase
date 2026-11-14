package com.sbase.postgres;

import com.sbase.config.PostgreSQLProperties;
import com.sbase.resilience.CircuitBreaker;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

/** Pooled PostgreSQL connectivity with a fail-fast breaker around connection acquisition. */
@Component
public class PostgreSQLClient {
    private final PostgreSQLProperties properties;
    private final DataSource dataSource;
    private final CircuitBreaker breaker = new CircuitBreaker(4, Duration.ofSeconds(20));

    public PostgreSQLClient(PostgreSQLProperties properties, @Qualifier("postgresDataSource") DataSource dataSource) {
        this.properties = properties;
        this.dataSource = dataSource;
    }

    public Connection openConnection() throws SQLException {
        try {
            return breaker.execute(() -> {
                try {
                    Connection connection = dataSource.getConnection();
                    try (var statement = connection.createStatement();
                         var result = statement.executeQuery("SELECT 1")) {
                        if (!result.next()) throw new SQLException("PostgreSQL validation query returned no rows");
                    } catch (SQLException failure) {
                        try { connection.close(); } catch (SQLException closeFailure) { failure.addSuppressed(closeFailure); }
                        throw new PostgreSQLConnectionFailure(failure);
                    }
                    return connection;
                } catch (SQLException failure) {
                    throw new PostgreSQLConnectionFailure(failure);
                }
            });
        } catch (PostgreSQLConnectionFailure failure) {
            throw failure.sqlException;
        }
    }

    public boolean isHealthy() {
        try (Connection connection = openConnection()) { return connection.isValid(2); }
        catch (SQLException | CircuitBreaker.CircuitOpenException ex) { return false; }
    }

    public String jdbcUrl() { return properties.jdbcUrl(); }
    public String username() { return properties.username(); }
    public String password() { return properties.password(); }

    private static final class PostgreSQLConnectionFailure extends RuntimeException {
        private final SQLException sqlException;
        private PostgreSQLConnectionFailure(SQLException cause) { super(cause); this.sqlException = cause; }
    }
}
