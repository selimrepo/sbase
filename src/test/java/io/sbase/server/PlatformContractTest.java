package io.sbase.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PlatformContractTest {
    @Test
    void platformIdentityIsStable() {
        assertEquals("SBase Data Science Server", Main$.MODULE$.ApiInfoName());
    }

    @Test
    void authenticationIsNotPartOfTheInitialHttpContract() {
        var routes = new String[] {"/health", "/api/v1", "/api/v1/ready"};
        assertTrue(java.util.Arrays.stream(routes).noneMatch(path ->
            path.contains("login") || path.contains("auth")));
    }
}
