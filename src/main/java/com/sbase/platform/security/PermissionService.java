package com.sbase.platform.security;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import java.util.*;
@Service
public class PermissionService {
 private final JdbcTemplate db;
 public PermissionService(JdbcTemplate db){this.db=db;}
 public String subject(Authentication auth){
  if(auth==null||!auth.isAuthenticated()) throw new org.springframework.security.access.AccessDeniedException("Authentication required");
  return auth.getName();
 }
 public boolean has(String subject,String permission){
  return db.query("SELECT 1 FROM sbase_user_roles ur JOIN sbase_role_permissions rp ON rp.role_id=ur.role_id WHERE ur.subject=? AND rp.permission=? LIMIT 1",rs->rs.next(),subject,permission);
 }
 public void require(Authentication auth,String permission){
  String sub=subject(auth);
  boolean mapped=db.query("SELECT 1 FROM sbase_user_roles ur JOIN sbase_role_permissions rp ON rp.role_id=ur.role_id WHERE ur.subject=? AND rp.permission=? LIMIT 1",rs->rs.next(),sub,permission);
  boolean jwt=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN")||a.getAuthority().equals("PERM_"+permission));
  if(!mapped&&!jwt) throw new org.springframework.security.access.AccessDeniedException("Missing permission: "+permission);
 }
}
