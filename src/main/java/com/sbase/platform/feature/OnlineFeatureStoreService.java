package com.sbase.platform.feature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class OnlineFeatureStoreService {
    private static final UUID LEGACY_TENANT=UUID.fromString("00000000-0000-0000-0000-000000000001");
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    public OnlineFeatureStoreService(JdbcTemplate jdbc,ObjectMapper mapper){this.jdbc=jdbc;this.mapper=mapper;}

    @Transactional
    public void upsert(String featureSet,String entityKey,int featureVersion,Instant eventTime,Map<String,Object> values) {
        upsert(LEGACY_TENANT,featureSet,entityKey,featureVersion,eventTime,values);
    }
    @Transactional
    public void upsert(UUID tenantId,String featureSet,String entityKey,int featureVersion,Instant eventTime,Map<String,Object> values) {
        validate(featureSet,entityKey,featureVersion,values);
        try {
            String json=mapper.writeValueAsString(values);
            jdbc.update("""
                INSERT INTO online_feature_values(tenant_id,feature_set,entity_key,feature_version,feature_values,event_time,materialized_at)
                VALUES(?,?,?,?,CAST(? AS jsonb),COALESCE(?,now()),now())
                ON CONFLICT(feature_set, entity_key) DO UPDATE SET
                    tenant_id=EXCLUDED.tenant_id,
                    feature_version=EXCLUDED.feature_version,
                    feature_values=EXCLUDED.feature_values,
                    event_time=EXCLUDED.event_time,
                    materialized_at=now()
                WHERE EXCLUDED.event_time >= online_feature_values.event_time
                """,tenantId,featureSet,entityKey,featureVersion,json,eventTime==null?null:java.sql.Timestamp.from(eventTime));
        } catch(JsonProcessingException ex) {
            throw new IllegalArgumentException("Feature values are not valid JSON",ex);
        }
    }
    @Transactional(readOnly=true)
    public Optional<Map<String,Object>> get(String featureSet,String entityKey) {
        return get(LEGACY_TENANT,featureSet,entityKey);
    }
    @Transactional(readOnly=true)
    public Optional<Map<String,Object>> get(UUID tenantId,String featureSet,String entityKey) {
        return jdbc.query("""
            SELECT feature_values::text FROM online_feature_values
            WHERE tenant_id=? AND feature_set=? AND entity_key=?
            """,rs->{
                if(!rs.next()) return Optional.empty();
                try {
                    return Optional.of(mapper.readValue(rs.getString(1),
                        mapper.getTypeFactory().constructMapType(Map.class,String.class,Object.class)));
                } catch(Exception ex) { throw new IllegalStateException("Stored feature vector is not valid JSON",ex); }
            },tenantId,featureSet,entityKey);
    }
    @Transactional(readOnly=true)
    public long count(String featureSet){return count(LEGACY_TENANT,featureSet);}
    @Transactional(readOnly=true)
    public long count(UUID tenantId,String featureSet) {
        return jdbc.queryForObject("SELECT count(*) FROM online_feature_values WHERE tenant_id=? AND feature_set=?",
            Long.class,tenantId,featureSet);
    }
    private static void validate(String featureSet,String entityKey,int version,Map<String,Object> values) {
        if(featureSet==null||!featureSet.matches("[A-Za-z0-9_.-]{1,160}")) throw new IllegalArgumentException("Invalid feature set name");
        if(entityKey==null||entityKey.isBlank()||entityKey.length()>512) throw new IllegalArgumentException("Entity key must contain 1..512 characters");
        if(version<1) throw new IllegalArgumentException("Feature version must be positive");
        if(values==null||values.isEmpty()) throw new IllegalArgumentException("Feature values cannot be empty");
    }
}
