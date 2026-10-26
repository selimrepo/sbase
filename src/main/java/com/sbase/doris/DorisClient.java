package com.sbase.doris;

import com.sbase.config.DorisProperties;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
public class DorisClient {
    private final DorisProperties properties;

    public DorisClient(DorisProperties properties) {
        this.properties = properties;
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(properties.jdbcUrl(), properties.username(), properties.password());
    }

    public boolean isHealthy() {
        try (Connection connection = openConnection();
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT 1")) {
            return result.next() && result.getInt(1) == 1;
        } catch (SQLException ex) {
            return false;
        }
    }

    public String jdbcUrl() { return properties.jdbcUrl(); }
    public String username() { return properties.username(); }
    public String password() { return properties.password(); }
}
