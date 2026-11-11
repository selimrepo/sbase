package com.sbase.platform.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
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
        Authentication auth=SecurityContextHolder.getContext().getAuthentication();
        String identity=auth!=null && auth.isAuthenticated() ? "user:"+auth.getName() : "ip:"+request.getRemoteAddr();
        String key=identity+":"+request.getRequestURI().replaceAll("/[0-9a-fA-F-]{36}","/{id}");
        Double remaining=jdbc.queryForObject("""
            INSERT INTO api_rate_limit_buckets(bucket_key,tokens,updated_at)
            VALUES(?,?,now())
            ON CONFLICT(bucket_key) DO UPDATE SET
              tokens=LEAST(?,api_rate_limit_buckets.tokens +
                GREATEST(0,EXTRACT(EPOCH FROM (now()-api_rate_limit_buckets.updated_at))) * ?) - 1,
              updated_at=now()
            RETURNING tokens
            """,Double.class,key,capacity-1,capacity,refillPerSecond);
        if(remaining==null || remaining<0) { response.setStatus(429); response.setHeader("Retry-After","1"); return; }
        chain.doFilter(request,response);
    }
}
