package com.sbase.doris;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;

@RestController
@RequestMapping("/api/doris")
public class DorisHealthController {
    private final DorisClient doris;

    public DorisHealthController(DorisClient doris) { this.doris = doris; }

    @GetMapping("/health")
    public Map<String, Object> health() {
        boolean connected = doris.isHealthy();
        if (!connected) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Doris is unavailable");
        return Map.of("connected", true, "database", "Apache Doris");
    }
}
