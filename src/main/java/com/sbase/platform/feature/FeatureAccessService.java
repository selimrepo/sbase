package com.sbase.platform.feature;

import com.sbase.platform.security.PermissionService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

@Service
public class FeatureAccessService {
    private final JdbcTemplate db;
    private final PermissionService permissions;

    public FeatureAccessService(@Qualifier("jdbcTemplate") JdbcTemplate db, PermissionService permissions) {
        this.db = db;
        this.permissions = permissions;
    }

    private record FeatureRow(java.util.UUID id, String lifecycle, String owner) { }

    public void require(Authentication authentication, String featureSet, boolean write) {
        String subject = permissions.subject(authentication);
        permissions.require(authentication, write ? "features:write" : "features:read");
        FeatureRow row = db.query("""
            SELECT id, lifecycle_state, owner_subject FROM feature_sets WHERE name=?
            """, rs -> rs.next() ? new FeatureRow(rs.getObject("id", java.util.UUID.class),
                rs.getString("lifecycle_state"), rs.getString("owner_subject")) : null, featureSet);
        if (row == null) throw new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.NOT_FOUND, "Feature set not found");
        if (write && !"ACTIVE".equalsIgnoreCase(row.lifecycle()))
            throw new org.springframework.security.access.AccessDeniedException("Only ACTIVE feature sets can be materialized");
        boolean admin = authentication.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin")) || permissions.has(subject, "platform:admin");
        if (subject.equals(row.owner()) || admin) return;
        Integer grants = db.queryForObject("SELECT count(*) FROM feature_access_grants WHERE feature_set_id=?",
            Integer.class, row.id());
        if (grants == null || grants == 0)
            throw new org.springframework.security.access.AccessDeniedException(
                row.owner() == null
                    ? "Feature set has no owner; administrator access is required"
                    : "Feature-set owner has not granted access");
        String grant = db.query("""
            SELECT g.permission FROM feature_access_grants g
            JOIN feature_sets f ON f.id=g.feature_set_id
            WHERE f.name=? AND g.subject=?
            """, rs -> rs.next() ? rs.getString(1) : null, featureSet, subject);
        if (!"ADMIN".equals(grant) && !(write ? "WRITE".equals(grant) : ("READ".equals(grant) || "WRITE".equals(grant)))) {
            throw new org.springframework.security.access.AccessDeniedException("Feature-set access denied");
        }
    }
}
