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

    public void require(Authentication authentication, String featureSet, boolean write) {
        String subject = permissions.subject(authentication);
        permissions.require(authentication, write ? "features:write" : "features:read");
        Integer grants = db.queryForObject("""
            SELECT count(*) FROM feature_access_grants g
            JOIN feature_sets f ON f.id=g.feature_set_id
            WHERE f.name=?
            """, Integer.class, featureSet);
        if (grants == null || grants == 0) return; // Preserve legacy feature sets until ACLs are explicitly configured.
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
