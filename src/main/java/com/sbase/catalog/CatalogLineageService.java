package com.sbase.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.tenant.TenantContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

@Service
public class CatalogLineageService {
    private static final Set<String> ASSET_TYPES=Set.of("DATA_SOURCE","CONNECTOR","TABLE","PIPELINE","PIPELINE_STEP","FEATURE_SET","MODEL","REPORT","REPORT_FIELD","DASHBOARD","WIDGET");
    private static final Set<String> EDGE_TYPES=Set.of("CONTAINS","READS_FROM","DERIVES_FROM","WRITES_TO","CONSUMES","USES_COLUMN","USES_REPORT","DISPLAYS","PUBLISHES","DEPENDS_ON");
    private static final Set<String> EVIDENCE_TYPES=Set.of("PARSER","PIPELINE_DEFINITION","CONNECTOR_DISCOVERY","USER_DECLARED","RUNTIME_OBSERVED","BI_DEFINITION");
    private static final Set<String> CLASSIFICATIONS=Set.of("PUBLIC","INTERNAL","CONFIDENTIAL","RESTRICTED");
    private final CatalogAssetRepository assets;
    private final CatalogColumnRepository columns;
    private final LineageEdgeRepository edges;
    private final TenantContext tenants;
    private final ObjectMapper mapper;

    public CatalogLineageService(CatalogAssetRepository assets,CatalogColumnRepository columns,LineageEdgeRepository edges,
            TenantContext tenants,ObjectMapper mapper) {
        this.assets=assets; this.columns=columns; this.edges=edges; this.tenants=tenants; this.mapper=mapper;
    }

    @Transactional
    public AssetView createAsset(CreateAsset request,Authentication auth) {
        tenants.requireWrite(auth);
        UUID tenant=tenants.tenantId(auth);
        String type=normalize(request.assetType());
        if(!ASSET_TYPES.contains(type)) throw bad("Unsupported asset type");
        if(request.externalKey().length()>512) throw bad("External key is too long");
        if(assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,type,request.externalKey()).isPresent())
            throw new ResponseStatusException(HttpStatus.CONFLICT,"Catalog asset already exists");
        CatalogAssetEntity entity=new CatalogAssetEntity(tenant,type,request.externalKey(),request.displayName().trim(),tenants.subject(auth));
        entity.updateGovernance(trim(request.description()),trim(request.ownerTeam()),trim(request.domain()),
                classification(request.classification()),writeTags(request.tags()),writeMetadata(request.metadata()));
        if(request.namespace()!=null || request.physicalName()!=null)
            entity.setPhysicalLocation(trim(request.namespace()),trim(request.physicalName()));
        if(request.parentId()!=null) {
            CatalogAssetEntity parent=requireAsset(tenant,request.parentId());
            entity.setParentId(parent.getId());
        }
        return view(assets.save(entity),null);
    }

    @Transactional
    public AssetView registerColumn(UUID tableId,CreateColumn request,Authentication auth) {
        tenants.requireWrite(auth);
        UUID tenant=tenants.tenantId(auth);
        CatalogAssetEntity table=requireAsset(tenant,tableId);
        if(!"TABLE".equals(table.getAssetType())) throw bad("Columns can only belong to TABLE assets");
        if(!request.name().matches("[A-Za-z_][A-Za-z0-9_$]{0,254}")) throw bad("Invalid column name");
        String external=table.getExternalKey()+":"+request.name().toLowerCase(Locale.ROOT);
        if(assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"COLUMN",external).isPresent())
            throw new ResponseStatusException(HttpStatus.CONFLICT,"Column is already registered");
        int ordinal=request.ordinalPosition()==null
                ? columns.findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(tenant,tableId).size()+1
                : request.ordinalPosition();
        if(ordinal<1 || ordinal>100000) throw bad("Invalid column ordinal position");
        CatalogAssetEntity column=new CatalogAssetEntity(tenant,"COLUMN",external,request.name(),tenants.subject(auth));
        column.setParentId(tableId);
        column.updateGovernance(request.description(),request.ownerTeam(),table.getDomain(),
                classification(request.classification()==null?table.getClassification():request.classification()),
                writeTags(request.tags()),writeMetadata(request.metadata()));
        column=assets.save(column);
        columns.save(new CatalogColumnEntity(column.getId(),tenant,tableId,ordinal,request.dataType().trim(),request.nullable()==null || request.nullable()));
        saveEdge(tenant,tableId,column.getId(),"CONTAINS","USER_DECLARED","column:"+column.getId(),null,BigDecimal.ONE,1,null);
        return view(column,columns.findById(column.getId()).orElseThrow());
    }

    @Transactional(readOnly=true)
    public List<AssetView> list(String type,Integer requestedLimit,Authentication auth) {
        UUID tenant=tenants.tenantId(auth);
        int limit=requestedLimit==null?100:Math.max(1,Math.min(200,requestedLimit));
        List<CatalogAssetEntity> result;
        if(type==null || type.isBlank()) result=assets.findAllByTenantIdOrderByUpdatedAtDesc(tenant,PageRequest.of(0,limit));
        else {
            String normalized=normalize(type);
            if(!ASSET_TYPES.contains(normalized) && !"COLUMN".equals(normalized)) throw bad("Unknown asset type");
            result=assets.findAllByTenantIdAndAssetTypeOrderByUpdatedAtDesc(tenant,normalized,PageRequest.of(0,limit));
        }
        return result.stream().map(a->view(a,a.getAssetType().equals("COLUMN")?columns.findById(a.getId()).orElse(null):null)).toList();
    }

    @Transactional(readOnly=true)
    public List<AssetView> columns(UUID tableId,Authentication auth) {
        UUID tenant=tenants.tenantId(auth);
        CatalogAssetEntity table=requireAsset(tenant,tableId);
        if(!"TABLE".equals(table.getAssetType())) throw bad("Columns can only be listed for TABLE assets");
        return columns.findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(tenant,tableId).stream()
            .map(col->assets.findByIdAndTenantId(col.getId(),tenant).map(a->view(a,col)).orElse(null))
            .filter(Objects::nonNull).toList();
    }

    @Transactional(readOnly=true)
    public AssetView get(UUID id,Authentication auth) {
        UUID tenant=tenants.tenantId(auth);
        CatalogAssetEntity asset=requireAsset(tenant,id);
        return view(asset,"COLUMN".equals(asset.getAssetType())?columns.findById(id).orElse(null):null);
    }

    @Transactional
    public AssetView updateGovernance(UUID id,GovernanceUpdate request,Authentication auth) {
        tenants.requireWrite(auth);
        UUID tenant=tenants.tenantId(auth);
        CatalogAssetEntity asset=requireAsset(tenant,id);
        requireOwnerOrAdmin(asset,auth);
        asset.updateGovernance(trim(request.description()),trim(request.ownerTeam()),trim(request.domain()),
                classification(request.classification()),writeTags(request.tags()),writeMetadata(request.metadata()));
        if(request.ownerSubject()!=null && !request.ownerSubject().isBlank()) asset.setOwnerSubject(request.ownerSubject().trim());
        if(request.lifecycleState()!=null) {
            String state=normalize(request.lifecycleState());
            if(!Set.of("ACTIVE","CERTIFIED","DEPRECATED","DRAFT").contains(state)) throw bad("Invalid lifecycle state");
            asset.setLifecycleState(state);
        }
        return view(asset,columns.findById(id).orElse(null));
    }

    @Transactional
    public EdgeView connect(CreateEdge request,Authentication auth) {
        tenants.requireWrite(auth);
        UUID tenant=tenants.tenantId(auth);
        requireAsset(tenant,request.fromAssetId()); requireAsset(tenant,request.toAssetId());
        if(request.fromAssetId().equals(request.toAssetId())) throw bad("Self-referencing lineage edges are not allowed");
        String type=normalize(request.edgeType()), evidence=normalize(request.evidenceType());
        if(!EDGE_TYPES.contains(type) || !EVIDENCE_TYPES.contains(evidence)) throw bad("Unsupported edge or evidence type");
        BigDecimal confidence=request.confidence()==null?BigDecimal.ONE:request.confidence();
        if(confidence.compareTo(BigDecimal.ZERO)<0 || confidence.compareTo(BigDecimal.ONE)>0) throw bad("Confidence must be between 0 and 1");
        LineageEdgeEntity edge=saveEdge(tenant,request.fromAssetId(),request.toAssetId(),type,evidence,
                trim(request.evidenceRef()),trim(request.expressionText()),confidence,
                request.effectiveVersion()==null?1:request.effectiveVersion(),request.runId());
        return edgeView(edge);
    }

    @Transactional(readOnly=true)
    public GraphView graph(UUID rootId,String direction,Integer requestedDepth,Authentication auth) {
        UUID tenant=tenants.tenantId(auth);
        requireAsset(tenant,rootId);
        String dir=direction==null?"BOTH":normalize(direction);
        if(!Set.of("UPSTREAM","DOWNSTREAM","BOTH").contains(dir)) throw bad("direction must be UPSTREAM, DOWNSTREAM or BOTH");
        int depth=requestedDepth==null?3:Math.max(1,Math.min(8,requestedDepth));
        return traverse(tenant,rootId,dir,depth);
    }

    @Transactional(readOnly=true)
    public GraphView impact(UUID assetId,Integer requestedDepth,Authentication auth) {
        UUID tenant=tenants.tenantId(auth);
        requireAsset(tenant,assetId);
        int depth=requestedDepth==null?6:Math.max(1,Math.min(8,requestedDepth));
        return traverse(tenant,assetId,"DOWNSTREAM",depth);
    }

    private GraphView traverse(UUID tenant,UUID root,String direction,int depth) {
        final int maxNodes=500,maxEdges=2000;
        LinkedHashMap<UUID,CatalogAssetEntity> found=new LinkedHashMap<>();
        LinkedHashMap<UUID,LineageEdgeEntity> foundEdges=new LinkedHashMap<>();
        Set<UUID> visited=new LinkedHashSet<>(); Set<UUID> frontier=new LinkedHashSet<>(); frontier.add(root);
        found.put(root,requireAsset(tenant,root)); boolean truncated=false;
        for(int level=0;level<depth && !frontier.isEmpty();level++) {
            List<UUID> ids=List.copyOf(frontier); frontier=new LinkedHashSet<>();
            List<LineageEdgeEntity> batch=new ArrayList<>();
            if(direction.equals("DOWNSTREAM") || direction.equals("BOTH")) batch.addAll(edges.findAllByTenantIdAndFromAssetIdIn(tenant,ids));
            if(direction.equals("UPSTREAM") || direction.equals("BOTH")) batch.addAll(edges.findAllByTenantIdAndToAssetIdIn(tenant,ids));
            for(LineageEdgeEntity edge:batch) {
                if(foundEdges.size()>=maxEdges) {truncated=true;break;}
                foundEdges.putIfAbsent(edge.getId(),edge);
                UUID next=(direction.equals("UPSTREAM"))?edge.getFromAssetId():edge.getToAssetId();
                if(direction.equals("BOTH")) next=ids.contains(edge.getFromAssetId())?edge.getToAssetId():edge.getFromAssetId();
                if(!visited.contains(next) && !found.containsKey(next)) {
                    if(found.size()>=maxNodes) {truncated=true;continue;}
                    frontier.add(next);
                }
            }
            visited.addAll(ids);
            if(!frontier.isEmpty()) {
                List<CatalogAssetEntity> nodes=assets.findAllByTenantIdAndIdIn(tenant,List.copyOf(frontier));
                for(CatalogAssetEntity node:nodes) found.put(node.getId(),node);
                frontier.removeIf(id->!found.containsKey(id));
            }
            if(truncated) break;
        }
        return new GraphView(root,direction,depth,truncated,
                found.values().stream().map(a->view(a,columns.findById(a.getId()).orElse(null))).toList(),
                foundEdges.values().stream().map(this::edgeView).toList());
    }

    private LineageEdgeEntity saveEdge(UUID tenant,UUID from,UUID to,String type,String evidence,String evidenceRef,
            String expression,BigDecimal confidence,long version,UUID runId) {
        String fingerprint=sha256(tenant+"|"+from+"|"+to+"|"+type+"|"+evidence+"|"+Objects.toString(evidenceRef,"")+"|"+version);
        return edges.findByTenantIdAndEdgeFingerprint(tenant,fingerprint).orElseGet(() ->
            edges.save(new LineageEdgeEntity(tenant,from,to,type,evidence,evidenceRef,expression,confidence,version,runId,fingerprint)));
    }

    private CatalogAssetEntity requireAsset(UUID tenant,UUID id) {
        return assets.findByIdAndTenantId(id,tenant).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Catalog asset not found"));
    }
    private void requireOwnerOrAdmin(CatalogAssetEntity asset,Authentication auth) {
        boolean admin=auth.getAuthorities().stream().anyMatch(a->Set.of("ROLE_ADMIN","PERM_platform:admin").contains(a.getAuthority()));
        if(!admin && !asset.getOwnerSubject().equals(tenants.subject(auth))) throw new AccessDeniedException("Only the asset owner or platform administrator can change governance metadata");
    }
    private AssetView view(CatalogAssetEntity a,CatalogColumnEntity c) {
        return new AssetView(a.getId(),a.getAssetType(),a.getExternalKey(),a.getDisplayName(),a.getNamespace(),a.getPhysicalName(),
            a.getDescription(),a.getOwnerSubject(),a.getOwnerTeam(),a.getDomain(),a.getClassification(),readTags(a.getTagsJson()),
            readMap(a.getMetadataJson()),a.getParentId(),a.getLifecycleState(),a.getSchemaFingerprint(),a.getCreatedAt(),a.getUpdatedAt(),
            c==null?null:c.getOrdinalPosition(),c==null?null:c.getDataType(),c==null?null:c.isNullable());
    }
    private EdgeView edgeView(LineageEdgeEntity e) {
        return new EdgeView(e.getId(),e.getFromAssetId(),e.getToAssetId(),e.getEdgeType(),e.getEvidenceType(),e.getEvidenceRef(),
            e.getExpressionText(),e.getConfidence(),e.getObservedAt(),e.getEffectiveVersion(),e.getRunId());
    }
    private String writeTags(List<String> tags) {
        List<String> normalized=tags==null?List.of():tags.stream().filter(Objects::nonNull).map(String::trim).filter(s->!s.isBlank()).distinct().limit(50).toList();
        if(normalized.stream().anyMatch(s->s.length()>80)) throw bad("Tag exceeds 80 characters");
        return write(normalized);
    }
    private String writeMetadata(Map<String,Object> metadata) {
        Map<String,Object> value=metadata==null?Map.of():metadata;
        String json=write(value); if(json.length()>16000) throw bad("Metadata exceeds 16 KB"); return json;
    }
    private String write(Object value) { try{return mapper.writeValueAsString(value);}catch(Exception e){throw bad("Invalid JSON metadata");} }
    private List<String> readTags(String json) { try{return mapper.readValue(json,new TypeReference<List<String>>(){});}catch(Exception e){return List.of();} }
    private Map<String,Object> readMap(String json) { try{return mapper.readValue(json,new TypeReference<Map<String,Object>>(){});}catch(Exception e){return Map.of();} }
    private String classification(String value) {
        String v=value==null||value.isBlank()?"INTERNAL":normalize(value);
        if(!CLASSIFICATIONS.contains(v)) throw bad("Invalid data classification"); return v;
    }
    private String normalize(String value){return value==null?"":value.trim().toUpperCase(Locale.ROOT);}
    private String trim(String value){return value==null||value.isBlank()?null:value.trim();}
    private String sha256(String value) {
        try {byte[] hash=MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));return HexFormat.of().formatHex(hash);}
        catch(Exception e){throw new IllegalStateException("SHA-256 is unavailable",e);}
    }
    private ResponseStatusException bad(String message){return new ResponseStatusException(HttpStatus.BAD_REQUEST,message);}

    public record CreateAsset(String assetType,String externalKey,String displayName,String namespace,String physicalName,
            String description,String ownerTeam,String domain,String classification,List<String> tags,Map<String,Object> metadata,UUID parentId) {}
    public record CreateColumn(String name,String dataType,Boolean nullable,Integer ordinalPosition,String description,
            String ownerTeam,String classification,List<String> tags,Map<String,Object> metadata) {}
    public record GovernanceUpdate(String description,String ownerSubject,String ownerTeam,String domain,String classification,
            List<String> tags,Map<String,Object> metadata,String lifecycleState) {}
    public record CreateEdge(UUID fromAssetId,UUID toAssetId,String edgeType,String evidenceType,String evidenceRef,
            String expressionText,BigDecimal confidence,Long effectiveVersion,UUID runId) {}
    public record AssetView(UUID id,String assetType,String externalKey,String displayName,String namespace,String physicalName,
            String description,String ownerSubject,String ownerTeam,String domain,String classification,List<String> tags,
            Map<String,Object> metadata,UUID parentId,String lifecycleState,String schemaFingerprint,Instant createdAt,Instant updatedAt,
            Integer ordinalPosition,String dataType,Boolean nullable) {}
    public record EdgeView(UUID id,UUID fromAssetId,UUID toAssetId,String edgeType,String evidenceType,String evidenceRef,
            String expressionText,BigDecimal confidence,Instant observedAt,long effectiveVersion,UUID runId) {}
    public record GraphView(UUID rootId,String direction,int depth,boolean truncated,List<AssetView> nodes,List<EdgeView> edges) {}
}
