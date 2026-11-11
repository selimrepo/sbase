package com.sbase.platform.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;

class PlatformSecurityConfigTest {

    @Test
    void mapsScopeRolesAndPermissionsToAuthorities() {
        Jwt jwt = Jwt.withTokenValue("test-token")
            .header("alg", "none")
            .claim("sub", "integration-user")
            .claim("scope", "dataset:read pipeline:run")
            .claim("roles", List.of("admin", "monitoring"))
            .claim("permissions", List.of("bi:read", "ingestion:write"))
            .build();

        JwtAuthenticationConverter converter = new PlatformSecurityConfig().jwtAuthenticationConverter();

        var authentication = converter.convert(jwt);

        assertThat(authentication).isNotNull();
        assertThat(authentication.getAuthorities())
            .extracting("authority")
            .contains(
                "SCOPE_dataset:read",
                "SCOPE_pipeline:run",
                "ROLE_ADMIN",
                "ROLE_MONITORING",
                "PERM_bi:read",
                "PERM_ingestion:write");
    }

    @Test
    void ignoresBlankScopeEntries() {
        Jwt jwt = Jwt.withTokenValue("test-token")
            .header("alg", "none")
            .claim("sub", "integration-user")
            .claim("scope", "dataset:read  pipeline:run ")
            .build();

        var authentication = new PlatformSecurityConfig().jwtAuthenticationConverter().convert(jwt);

        assertThat(authentication).isNotNull();
        assertThat(authentication.getAuthorities())
            .extracting("authority")
            .containsExactlyInAnyOrder("SCOPE_dataset:read", "SCOPE_pipeline:run");
    }
}
