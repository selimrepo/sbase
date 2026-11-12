package com.sbase.platform;

import com.sbase.platform.security.PermissionService;
import com.sbase.platform.tenant.TenantContext;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DashboardLayoutControllerTenantIsolationTest {
    @Test
    void refusesLayoutMutationWhenDashboardBelongsToAnotherTenant() {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        PermissionService permissions=mock(PermissionService.class);
        TenantContext tenants=mock(TenantContext.class);
        UUID tenant=UUID.randomUUID(), dashboard=UUID.randomUUID();
        Jwt jwt=Jwt.withTokenValue("test-token").header("alg","none").claim("sub","alice").build();
        JwtAuthenticationToken auth=new JwtAuthenticationToken(jwt);
        when(tenants.tenantId(auth)).thenReturn(tenant);
        when(jdbc.queryForObject(anyString(),eq(Integer.class),eq(dashboard),eq(tenant),eq("alice"))).thenReturn(0);

        DashboardLayoutController controller=new DashboardLayoutController(jdbc,permissions,tenants);
        var request=new DashboardLayoutController.LayoutRequest(
            List.of(new DashboardLayoutController.WidgetPosition(UUID.randomUUID(),0,0,4,3)));

        assertThrows(ResponseStatusException.class,()->controller.saveLayout(dashboard,request,auth));
        verify(jdbc,never()).update(anyString(),any(Object[].class));
        verify(tenants).requireWrite(auth);
    }
}
