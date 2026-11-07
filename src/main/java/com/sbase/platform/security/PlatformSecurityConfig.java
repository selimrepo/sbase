package com.sbase.platform.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import java.util.*;
import java.util.stream.Collectors;

@Configuration
public class PlatformSecurityConfig {
 @Bean SecurityFilterChain platformFilterChain(HttpSecurity http) throws Exception {
  http.csrf(c -> c.disable()).authorizeHttpRequests(a -> a
   .requestMatchers("/actuator/health/**","/actuator/info").permitAll()
   .requestMatchers("/api/platform/bi/**").hasAnyAuthority("PERM_bi:read","PERM_bi:query","ROLE_ADMIN")
   .requestMatchers("/api/platform/schedules/**","/api/platform/features/**","/api/platform/ingestion/**").authenticated()
   .requestMatchers("/api/platform/**","/api/algorithms/**").authenticated()
   .anyRequest().permitAll())
   .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(jwtAuthenticationConverter())));
  return http.build();
 }
 @Bean JwtAuthenticationConverter jwtAuthenticationConverter() {
  var converter=new JwtAuthenticationConverter();
  converter.setJwtGrantedAuthoritiesConverter(jwt -> {
   Set<String> authorities=new HashSet<>();
   Object scope=jwt.getClaims().get("scope");
   if(scope instanceof String s) for(String v:s.split(" ")) authorities.add("SCOPE_"+v);
   Object roles=jwt.getClaims().get("roles");
   if(roles instanceof Collection<?> c) for(Object r:c) authorities.add("ROLE_"+String.valueOf(r).toUpperCase(Locale.ROOT));
   Object perms=jwt.getClaims().get("permissions");
   if(perms instanceof Collection<?> c) for(Object p:c) authorities.add("PERM_"+p);
   return authorities.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toSet());
  });
  return converter;
 }
}
