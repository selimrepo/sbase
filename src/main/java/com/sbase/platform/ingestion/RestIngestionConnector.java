package com.sbase.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

@Component
public class RestIngestionConnector implements IngestionConnector {
    private final DurableRecordSink sink; private final ObjectMapper mapper;
    public RestIngestionConnector(DurableRecordSink sink,ObjectMapper mapper){this.sink=sink;this.mapper=mapper;}
    @Override public String type(){return "rest";}
    @Override public ConnectorResult ingest(ConnectorRequest request){
        Map<String,String> o=request.options(); UUID id=UUID.fromString(required(o,"connectorId")); UUID tenantId=UUID.fromString(required(o,"tenantId"));
        String url=required(o,"url"); int max=Integer.parseInt(o.getOrDefault("maxPages","1"));
        String method=o.getOrDefault("method","GET").toUpperCase(Locale.ROOT);
        if(!method.equals("GET"))throw new IllegalArgumentException("Only GET is supported for safe REST source ingestion");
        HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        long read=0,written=0; String next=url; int page=0;
        try{
            while(next!=null && page++<=max){
                HttpRequest.Builder b=HttpRequest.newBuilder(URI.create(next)).timeout(Duration.ofSeconds(30)).GET()
                    .header("Accept","application/json");
                if(o.containsKey("bearerToken"))b.header("Authorization","Bearer "+o.get("bearerToken"));
                HttpResponse<String> response=client.send(b.build(),HttpResponse.BodyHandlers.ofString());
                if(response.statusCode()<200||response.statusCode()>=300)throw new IllegalStateException("REST source returned HTTP "+response.statusCode());
                Object root=mapper.readValue(response.body(),Object.class);
                List<?> values=root instanceof List<?> l?l:root instanceof Map<?,?> m && m.get("data") instanceof List<?> l?l:List.of(root);
                List<DurableRecordSink.Envelope> batch=new ArrayList<>();
                int index=0;for(Object value:values){@SuppressWarnings("unchecked") Map<String,Object> payload=value instanceof Map<?,?> m?(Map<String,Object>)m:Map.of("value",value);
                    requireTenantPayload(payload,tenantId);
                    Object key=payload.getOrDefault(o.getOrDefault("idField","id"),page+":"+index);
                    batch.add(new DurableRecordSink.Envelope("rest:"+url+":"+key,payload));index++;}
                written+=sink.write(id,"rest:"+url,batch,"rest:"+url,String.valueOf(page));
                read+=values.size();
                Object cursor=root instanceof Map<?,?> m?m.get(o.getOrDefault("nextPageField","next")):null;
                next=cursor==null?null:String.valueOf(cursor);
            }
        }catch(Exception e){throw new IllegalStateException("REST ingestion failed",e);}
        return new ConnectorResult(read,written,"REST JSON pages persisted to PostgreSQL landing sink");
    }
    static void requireTenantPayload(Map<String,Object> payload,UUID tenantId) {
        if(payload==null || tenantId==null || !payload.containsKey("tenant_id")
                || payload.get("tenant_id")==null
                || !tenantId.toString().equals(payload.get("tenant_id").toString())) {
            throw new org.springframework.security.access.AccessDeniedException(
                "REST source record must carry the authenticated connector tenant_id");
        }
    }

    private static String required(Map<String,String> o,String k){String v=o.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
}
