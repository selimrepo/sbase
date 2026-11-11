package com.sbase.doris;

import com.sbase.config.DorisProperties;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component
public class DorisClient {
    private final DorisProperties properties;
    private final DataSource dataSource;

    public DorisClient(DorisProperties properties, @Qualifier("dorisDataSource") DataSource dataSource) {
        this.properties = properties;
        this.dataSource = dataSource;
    }

    public Connection openConnection() throws SQLException {
        return dataSource.getConnection();
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
