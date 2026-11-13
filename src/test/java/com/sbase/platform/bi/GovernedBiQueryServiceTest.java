package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GovernedBiQueryServiceTest {
 private JdbcTemplate analytics;
 private DorisBiQueryService doris;
 private DorisTenantTablePolicy policy;
 private GovernedBiQueryService service;
 private final UUID tenant=UUID.fromString("11111111-1111-1111-1111-111111111111");
 @BeforeEach void setUp(){
  analytics=mock(JdbcTemplate.class);
  List<Map<String,Object>> columns=List.of(Map.of("COLUMN_NAME","tenant_id"),Map.of("COLUMN_NAME","region"),Map.of("COLUMN_NAME","revenue"),Map.of("COLUMN_NAME","created_at"));
  doReturn(columns).when(analytics).queryForList(anyString(),any(Object[].class));
  doris=mock(DorisBiQueryService.class);
  policy=mock(DorisTenantTablePolicy.class);
  when(policy.requireTenantScoped("analytics.sales")).thenReturn("analytics.sales");
  service=new GovernedBiQueryService(doris,policy,mock(JdbcTemplate.class),analytics,new ObjectMapper(),"analytics","analytics.sales",4,30000,5000,100000);
 }
 @Test void compilesTenantScopedAggregateQuery(){
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("region"),
   List.of(new GovernedBiQueryService.Metric("revenue","SUM","total")),List.of(),List.of(new GovernedBiQueryService.Sort("total","DESC")),100);
  String sql=service.compile(definition,tenant);
  assertTrue(sql.contains("SUM(revenue) AS total"));
  assertFalse(sql.contains("tenant_id"));
  assertTrue(sql.contains("GROUP BY region"));
  assertTrue(sql.endsWith("LIMIT 100"));
 }
 @Test void escapesFilterValuesInsteadOfAllowingSqlInjection(){
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of(),
   List.of(new GovernedBiQueryService.Metric("*","COUNT","total")),
   List.of(new GovernedBiQueryService.Filter("region","EQ","North' OR 1=1 --")),List.of(),50);
  String sql=service.compile(definition,tenant);
  assertTrue(sql.contains("'North'' OR 1=1 --'"));
  assertFalse(sql.contains("tenant_id"));
  assertFalse(sql.contains(" OR 1=1 -- LIMIT"));
 }
 @Test void rejectsTablesOutsideAllowList(){
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.private_data",List.of(),
   List.of(new GovernedBiQueryService.Metric("*","COUNT","total")),List.of(),List.of(),10);
  assertThrows(IllegalArgumentException.class,()->service.compile(definition,tenant));
 }
 @Test void rejectsSourcesWithoutTenantColumn(){
  doReturn(List.of(Map.of("COLUMN_NAME","region"),Map.of("COLUMN_NAME","revenue")))
   .when(analytics).queryForList(anyString(),any(Object[].class));
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("region"),
   List.of(new GovernedBiQueryService.Metric("revenue","SUM","total")),List.of(),List.of(),100);
  assertThrows(org.springframework.security.access.AccessDeniedException.class,()->service.compile(definition,tenant));
 }
 @Test void drillThroughUsesTheAuthenticatedTenantExecutionBoundary(){
  doReturn(List.of(Map.of("region","north"))).when(doris).query(eq(tenant),anyString());
  service.drillThrough(tenant,"alice","analytics.sales",List.of("region"),List.of(),25);
  var sql=org.mockito.ArgumentCaptor.forClass(String.class);
  verify(doris).query(eq(tenant),sql.capture());
  assertTrue(sql.getValue().contains("FROM analytics.sales"));
  assertFalse(sql.getValue().contains("tenant_id"));
 }
 @Test void preventsSelectingOrFilteringTheTenantDiscriminator(){
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("tenant_id"),
   List.of(new GovernedBiQueryService.Metric("*","COUNT","total")),List.of(),List.of(),10);
  assertThrows(IllegalArgumentException.class,()->service.compile(definition,tenant));
 }
 @Test void enforcesBoundedResultAndSafeIdentifiers(){
  var tooLarge=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of(),
   List.of(new GovernedBiQueryService.Metric("*","COUNT","total")),List.of(),List.of(),6000);
  assertThrows(IllegalArgumentException.class,()->service.compile(tooLarge,tenant));
  var unsafe=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("region; DROP TABLE sales"),
   List.of(new GovernedBiQueryService.Metric("revenue","SUM","total")),List.of(),List.of(),100);
  assertThrows(IllegalArgumentException.class,()->service.compile(unsafe,tenant));
 }
 @Test void queryResultsAndCacheEntriesAreIsolatedByTenant(){
  UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
  UUID tenantB=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
  var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("region"),
   List.of(new GovernedBiQueryService.Metric("revenue","SUM","total")),List.of(),List.of(),100);
  doReturn(List.of(Map.of("region","north","total",10))).when(doris).query(any(UUID.class),anyString());

  service.execute(tenantA,"alice",definition);
  service.execute(tenantB,"bob",definition);
  service.execute(tenantA,"alice",definition);

  var tenantCaptor=org.mockito.ArgumentCaptor.forClass(UUID.class);
  var sqlCaptor=org.mockito.ArgumentCaptor.forClass(String.class);
  verify(doris,times(2)).query(tenantCaptor.capture(),sqlCaptor.capture());
  assertEquals(List.of(tenantA,tenantB),tenantCaptor.getAllValues());
  assertTrue(sqlCaptor.getAllValues().stream().noneMatch(sql->sql.toLowerCase(Locale.ROOT).contains("tenant_id")));
 }

}
