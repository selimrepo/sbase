package com.sbase.platform;

import com.sbase.platform.bi.DorisBiAnalyticsService;
import com.sbase.platform.security.PermissionService;
import com.sbase.platform.tenant.TenantContext;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PlatformMetadataControllerTenantIsolationTest {
    private final JdbcTemplate db=mock(JdbcTemplate.class);
    private final DorisBiAnalyticsService analytics=mock(DorisBiAnalyticsService.class);
    private final PermissionService permissions=mock(PermissionService.class);
    private final TenantContext tenants=mock(TenantContext.class);
    private final Authentication auth=mock(Authentication.class);
    private final UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private final UUID tenantB=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private PlatformMetadataController controller(){return new PlatformMetadataController(db,analytics,permissions,tenants);}

    @Test
    void legacyExperimentListingIsTenantScoped() {
        when(tenants.tenantId(auth)).thenReturn(tenantA);
        when(auth.getName()).thenReturn("alice");
        when(db.queryForList(anyString(),eq(tenantA))).thenReturn(List.of());
        controller().experiments(auth);
        org.mockito.ArgumentCaptor<String> sql=org.mockito.ArgumentCaptor.forClass(String.class);
        verify(db).queryForList(sql.capture(),eq(tenantA));
        assertTrue(sql.getValue().contains("WHERE tenant_id=?"));
    }

    @Test
    void legacyScheduleCannotReferenceAnotherTenantsPipeline() {
        UUID pipeline=UUID.randomUUID();
        when(tenants.tenantId(auth)).thenReturn(tenantA);
        when(auth.getName()).thenReturn("alice");
        when(db.query(eq("SELECT EXISTS(SELECT 1 FROM pipelines WHERE id=? AND tenant_id=?)"),
            any(ResultSetExtractor.class),eq(pipeline),eq(tenantA))).thenReturn(false);
        var request=new PlatformMetadataController.Schedule("foreign-pipeline",pipeline,"0 0 * * * *");
        assertThrows(ResponseStatusException.class,()->controller().schedule(request,auth));
        verify(db,never()).update(startsWith("INSERT INTO scheduled_jobs"),any(Object[].class));
        assertNotEquals(tenantA,tenantB);
    }
}
