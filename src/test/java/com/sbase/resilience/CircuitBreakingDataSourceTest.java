package com.sbase.resilience;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CircuitBreakingDataSourceTest {
    @Test
    void observesConnectionFailuresFromPreparedStatementsAndOpensCircuit() throws Exception {
        DataSource pool = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeUpdate()).thenThrow(new SQLRecoverableException("connection lost", "08006"));

        CircuitBreakingDataSource guarded = new CircuitBreakingDataSource(pool, "postgres", 2,
                Duration.ofSeconds(30), 0, 0);
        for (int i = 0; i < 2; i++) {
            assertThrows(SQLRecoverableException.class,
                    () -> guarded.getConnection().prepareStatement("UPDATE t SET v=1").executeUpdate());
        }
        assertEquals(CircuitBreaker.State.OPEN, guarded.circuitState());
        assertThrows(SQLException.class, guarded::getConnection);
    }

    @Test
    void sqlConstraintErrorsDoNotTripConnectivityCircuit() throws Exception {
        DataSource pool = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeUpdate()).thenThrow(new SQLException("duplicate key", "23505"));

        CircuitBreakingDataSource guarded = new CircuitBreakingDataSource(pool, "postgres", 1,
                Duration.ofSeconds(30), 0, 0);
        assertThrows(SQLException.class,
                () -> guarded.getConnection().prepareStatement("INSERT INTO t VALUES (1)").executeUpdate());
        assertEquals(CircuitBreaker.State.CLOSED, guarded.circuitState());
    }
}
