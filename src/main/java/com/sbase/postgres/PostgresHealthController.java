package com.sbase.postgres;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;

@RestController
@RequestMapping("/api/postgres")
public class PostgresHealthController {
    private final PostgresClient postgres;

    public PostgresHealthController(PostgresClient postgres) { this.postgres = postgres; }

    @GetMapping("/health")
    public Map<String, Object> health() {
        boolean connected = postgres.isHealthy();
        if (!connected) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "PostgreSQL is unavailable");
        return Map.of("connected", true, "database", "PostgreSQL");
    }
}
