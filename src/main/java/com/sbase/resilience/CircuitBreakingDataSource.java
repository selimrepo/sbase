package com.sbase.resilience;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JDBC boundary breaker. It observes SQLExceptions from JDBC/JPA calls (not just
 * pool acquisition). Only connection acquisition is retried: replaying an arbitrary
 * SQL statement/transaction is unsafe because the server may have committed it.
 */
public final class CircuitBreakingDataSource implements DataSource, AutoCloseable {
    private final DataSource delegate;
    private final CircuitBreaker breaker;
    private final int acquisitionRetries;
    private final long baseBackoffMillis;

    public CircuitBreakingDataSource(DataSource delegate, String name, int threshold,
                                     Duration openDuration, int acquisitionRetries,
                                     long baseBackoffMillis) {
        this.delegate = Objects.requireNonNull(delegate);
        this.breaker = new CircuitBreaker(threshold, openDuration);
        this.acquisitionRetries = Math.max(0, Math.min(5, acquisitionRetries));
        this.baseBackoffMillis = Math.max(0, baseBackoffMillis);
    }

    @Override public Connection getConnection() throws SQLException {
        return acquire(() -> delegate.getConnection());
    }

    @Override public Connection getConnection(String username, String password) throws SQLException {
        return acquire(() -> delegate.getConnection(username, password));
    }

    private Connection acquire(SqlSupplier supplier) throws SQLException {
        SQLException last = null;
        for (int attempt = 0; attempt <= acquisitionRetries; attempt++) {
            try {
                Connection connection = breaker.execute(() -> {
                    try { return supplier.get(); }
                    catch (SQLException e) { throw new JdbcFailure(e); }
                });
                return (Connection) wrap(connection, Connection.class);
            } catch (JdbcFailure failure) {
                last = failure.sql;
            } catch (CircuitBreaker.CircuitOpenException open) {
                throw new SQLException("Database circuit is " + breaker.state(), open);
            }
            if (attempt < acquisitionRetries) {
                try { Thread.sleep(baseBackoffMillis * (1L << Math.min(attempt, 4))); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting to retry JDBC connection acquisition", interrupted);
                }
            }
        }
        throw last == null ? new SQLException("Unable to acquire database connection") : last;
    }

    private Object wrap(Object target, Class<?> jdbcType) {
        return Proxy.newProxyInstance(jdbcType.getClassLoader(), new Class<?>[]{jdbcType}, (proxy, method, args) -> {
            try {
                Object result = method.invoke(target, args);
                if (result instanceof Statement) return wrap(result, Statement.class);
                if (result instanceof ResultSet) return wrap(result, ResultSet.class);
                breaker.recordSuccess();
                return result;
            } catch (InvocationTargetException invocation) {
                Throwable cause = invocation.getCause();
                if (cause instanceof SQLException) breaker.recordFailure();
                throw cause;
            }
        });
    }

    @FunctionalInterface private interface SqlSupplier { Connection get() throws SQLException; }
    private static final class JdbcFailure extends RuntimeException {
        private final SQLException sql;
        private JdbcFailure(SQLException sql) { super(sql); this.sql = sql; }
    }

    public CircuitBreaker.State circuitState() { return breaker.state(); }
    @Override public java.io.PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
    @Override public void setLogWriter(java.io.PrintWriter out) throws SQLException { delegate.setLogWriter(out); }
    @Override public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
    @Override public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
    @Override public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException { return delegate.getParentLogger(); }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        return delegate.unwrap(iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }
    @Override public void close() {
        if (delegate instanceof AutoCloseable closeable) {
            try { closeable.close(); } catch (Exception e) { throw new IllegalStateException("Failed to close JDBC pool", e); }
        }
    }
}
