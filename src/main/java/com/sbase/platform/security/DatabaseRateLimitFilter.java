package com.sbase.platform.security;

import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class DatabaseRateLimitFilter extends OncePerRequestFilter {
    private final JdbcTemplate jdbc;
    private final int capacity;
    private final double refillPerSecond;
    public DatabaseRateLimitFilter(JdbcTemplate jdbc,
            @Value("${sbase.rate-limit.capacity:120}") int capacity,
            @Value("${sbase.rate-limit.refill-per-second:2}") double refillPerSecond) {
        this.jdbc=jdbc; this.capacity=Math.max(1,capacity); this.refillPerSecond=Math.max(0.01,refillPerSecond);
    }
    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)
            throws ServletException,IOException {
        Authentication auth=org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        String identity=auth!=null && auth.isAuthenticated() ? "user:"+auth.getName() : "ip:"+clientIp(request);
        String key=identity+":"+request.getRequestURI().replaceAll("/[0-9a-fA-F-]{36}","/{id}");
        Boolean allowed=jdbc.queryForObject("""
            INSERT INTO api_rate_limit_buckets(bucket_key,tokens,updated_at)
            VALUES(?,?,now())
            ON CONFLICT(bucket_key) DO UPDATE SET
              tokens=LEAST(?,api_rate_limit_buckets.tokens +
                GREATEST(0,EXTRACT(EPOCH FROM (now()-api_rate_limit_buckets.updated_at))) * ?),
              updated_at=now()
            RETURNING tokens
            """,Boolean.class,key,capacity-1,capacity,refillPerSecond);
        // Consume one token atomically; a zero balance is rejected.
        Double remaining=jdbc.queryForObject("SELECT tokens FROM api_rate_limit_buckets WHERE bucket_key=?",Double.class,key);
        if(remaining==null || remaining<0) { response.setStatus(429); response.setHeader("Retry-After","1"); return; }
        jdbc.update("UPDATE api_rate_limit_buckets SET tokens=GREATEST(0,tokens-1) WHERE bucket_key=?",key);
        chain.doFilter(request,response);
    }
    private String clientIp(HttpServletRequest request) {
        // Do not trust X-Forwarded-For here; use the socket peer unless a trusted proxy integration is configured.
        return request.getRemoteAddr();
    }
}
