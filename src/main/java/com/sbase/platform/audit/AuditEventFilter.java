package com.sbase.platform.audit;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class AuditEventFilter extends OncePerRequestFilter {
    private final JdbcTemplate jdbc;
    public AuditEventFilter(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)
            throws ServletException,IOException {
        try { chain.doFilter(request,response); }
        finally {
            var auth=SecurityContextHolder.getContext().getAuthentication();
            String subject=auth==null ? "anonymous" : auth.getName();
            UUID tenant=null;
            if(auth instanceof JwtAuthenticationToken jwt) {
                Object claim=jwt.getToken().getClaims().get("tenant_id");
                if(claim instanceof String value) try { tenant=UUID.fromString(value); } catch(IllegalArgumentException ignored) { }
            }
            String requestId=request.getHeader("X-Request-ID");
            if(requestId==null || !requestId.matches("[A-Za-z0-9._:-]{1,100}")) requestId=UUID.randomUUID().toString();
            response.setHeader("X-Request-ID",requestId);
            String outcome=response.getStatus()<400 ? "SUCCESS" : "FAILURE";
            jdbc.update("""
                INSERT INTO platform_audit_events(tenant_id,subject,action,resource_type,resource_id,outcome,request_id,source_ip,user_agent,details)
                VALUES(?,?,?,?,?,?,?,?,?,CAST(? AS jsonb))
                """,tenant,subject,request.getMethod()+" "+request.getRequestURI(),"HTTP",null,outcome,
                requestId,request.getRemoteAddr(),request.getHeader("User-Agent"),
                "{\"status\":"+response.getStatus()+"}");
        }
    }
}
