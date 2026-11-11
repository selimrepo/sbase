package com.sbase.platform.feature;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

@Service
public class PointInTimeFeatureService {
 private final JdbcTemplate db; private final ObjectMapper mapper;
 public PointInTimeFeatureService(@Qualifier("jdbcTemplate") JdbcTemplate db,ObjectMapper mapper){this.db=db;this.mapper=mapper;}
 @Transactional
 public void materialize(UUID tenant,String set,String entity,int version,Instant eventTime,Map<String,Object> values,String sourceRun) {
  if(version<1||set==null||!set.matches("[A-Za-z0-9_.-]{1,160}")||entity==null||entity.isBlank()||entity.length()>512||values==null||values.isEmpty())
   throw new IllegalArgumentException("Invalid feature materialization");
  Instant timestamp=eventTime==null?Instant.now():eventTime;
  String json=write(values);
  db.update("""
   INSERT INTO offline_feature_values(tenant_id,feature_set,entity_key,feature_version,feature_values,event_time,source_run_id)
   VALUES(?,?,?,?,CAST(? AS jsonb),?,?)
   ON CONFLICT(tenant_id,feature_set,entity_key,event_time,feature_version) DO NOTHING
   """,tenant,set,entity,version,json,timestamp,sourceRun==null?null:UUID.fromString(sourceRun));
  db.update("""
   INSERT INTO online_feature_values(tenant_id,feature_set,entity_key,feature_version,feature_values,event_time,materialized_at)
   VALUES(?,?,?,?,CAST(? AS jsonb),?,now())
   ON CONFLICT(tenant_id,feature_set,entity_key) DO UPDATE SET
    feature_version=EXCLUDED.feature_version,feature_values=EXCLUDED.feature_values,
    event_time=EXCLUDED.event_time,materialized_at=now()
   WHERE EXCLUDED.event_time>=online_feature_values.event_time
   """,tenant,set,entity,version,json,timestamp);
 }
 public Map<String,Object> asOf(UUID tenant,String set,String entity,Instant asOf) {
  return db.query("""
   SELECT feature_values::text,event_time,feature_version FROM offline_feature_values
   WHERE tenant_id=? AND feature_set=? AND entity_key=? AND event_time<=?
   ORDER BY event_time DESC,feature_version DESC LIMIT 1
   """,rs->{if(!rs.next())throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND,"No feature vector existed at requested event time");
    Map<String,Object> out=new LinkedHashMap<>();out.put("values",read(rs.getString(1)));out.put("eventTime",rs.getTimestamp(2).toInstant());out.put("featureVersion",rs.getInt(3));return out;},tenant,set,entity,asOf);
 }
 public Map<String,Object> parity(UUID tenant,String set,String entity,Instant asOf) {
  Map<String,Object> offline=asOf(tenant,set,entity,asOf);
  Map<String,Object> online=db.query("""
   SELECT feature_values::text,event_time,feature_version FROM online_feature_values
   WHERE tenant_id=? AND feature_set=? AND entity_key=?
   """,rs->{if(!rs.next())return null;Map<String,Object> x=new LinkedHashMap<>();x.put("values",read(rs.getString(1)));x.put("eventTime",rs.getTimestamp(2).toInstant());x.put("featureVersion",rs.getInt(3));return x;},tenant,set,entity);
  Map<String,Object> offValues=(Map<String,Object>)offline.get("values");
  Map<String,Object> onValues=online==null?Map.of():(Map<String,Object>)online.get("values");
  String oh=hash(offValues),nh=hash(onValues);boolean same=online!=null&&((Instant)online.get("eventTime")).compareTo(asOf)<=0&&oh.equals(nh);
  Map<String,Object> differences=new TreeMap<>();Set<String> keys=new TreeSet<>();keys.addAll(offValues.keySet());keys.addAll(onValues.keySet());
  for(String key:keys)if(!Objects.equals(offValues.get(key),onValues.get(key)))differences.put(key,Map.of("offline",String.valueOf(offValues.get(key)),"online",String.valueOf(onValues.get(key))));
  UUID id=UUID.randomUUID();db.update("""
   INSERT INTO sbase_feature_parity_checks(id,tenant_id,feature_set,entity_key,as_of,is_match,offline_hash,online_hash,differences_json)
   VALUES(?,?,?,?,?,?,?,?,CAST(? AS jsonb))
   """,id,tenant,set,entity,asOf,same,oh,online==null?null:nh,write(differences));
  return Map.of("id",id,"matches",same,"asOf",asOf,"offline",offline,"online",online==null?Map.of():online,"differences",differences);
 }
 private String hash(Map<String,Object> values){try{byte[] bytes=MessageDigest.getInstance("SHA-256").digest(write(new TreeMap<>(values)).getBytes(StandardCharsets.UTF_8));return HexFormat.of().formatHex(bytes);}catch(Exception e){throw new IllegalStateException(e);}}
 private String write(Object v){try{return mapper.writeValueAsString(v);}catch(Exception e){throw new IllegalArgumentException("Feature values are not JSON serializable",e);}}
 private Map<String,Object> read(String s){try{return mapper.readValue(s,mapper.getTypeFactory().constructMapType(Map.class,String.class,Object.class));}catch(Exception e){throw new IllegalStateException("Stored feature JSON is invalid",e);}}
}
