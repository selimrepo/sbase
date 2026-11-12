package com.sbase.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
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
    public void synchronizeDataset(com.sbase.catalog.DatasetEntity dataset) {
        UUID tenant=dataset.getTenantId();
        CatalogAssetEntity table=upsertAsset(tenant,"TABLE",dataset.getId().toString(),dataset.getName(),
            dataset.getOwnerSubject(),null);
        table.updateSourceDetails(dataset.getName(),dataset.getDescription(),dataset.getNamespace(),dataset.getTableName());
        table.setLifecycleState("ACTIVE");
        table.setSchemaFingerprint(sha256(Objects.toString(dataset.getSchemaJson(),"[]")));
        assets.save(table);
        JsonNode schema;
        try { schema=mapper.readTree(Objects.toString(dataset.getSchemaJson(),"[]")); }
        catch(Exception invalidSchema) { return; }
        if(schema==null || !schema.isArray()) return;
        Set<String> present=new HashSet<>(); int ordinal=0;
        for(JsonNode item:schema) {
            ordinal++;
            if(!item.isObject()) continue;
            String name=firstText(item,"name","columnName","field");
            if(name==null) name="column_"+ordinal;
            if(name.length()>255) continue;
            String type=firstText(item,"type","dataType");
            if(type==null) type="UNKNOWN";
            String key=dataset.getId()+":"+name.toLowerCase(Locale.ROOT);
            present.add(key);
            CatalogAssetEntity column=upsertAsset(tenant,"COLUMN",key,name,dataset.getOwnerSubject(),table.getId());
            column.setParentId(table.getId());
            column.setLifecycleState("ACTIVE");
            assets.save(column);
            boolean nullable=!item.has("nullable") || !item.get("nullable").isBoolean() || item.get("nullable").asBoolean();
            CatalogColumnEntity detail=columns.findById(column.getId()).orElse(null);
            if(detail==null) columns.save(new CatalogColumnEntity(column.getId(),tenant,table.getId(),ordinal,type,nullable));
            else detail.updateSchema(ordinal,type,nullable);
            saveEdge(tenant,table.getId(),column.getId(),"CONTAINS","USER_DECLARED",
                "dataset-schema:"+dataset.getId(),null,BigDecimal.ONE,1,null);
        }
        for(CatalogColumnEntity old:columns.findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(tenant,table.getId())) {
            CatalogAssetEntity oldAsset=assets.findByIdAndTenantId(old.getId(),tenant).orElse(null);
            if(oldAsset!=null && !present.contains(oldAsset.getExternalKey())) oldAsset.setLifecycleState("DEPRECATED");
        }
    }

    @Transactional
    public void deprecateDataset(UUID tenant,UUID datasetId) {
        deprecateAsset(tenant,"TABLE",datasetId.toString());
    }

    @Transactional
    public void deprecateAsset(UUID tenant,String type,String externalKey) {
        assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,type,externalKey)
            .ifPresent(asset->asset.setLifecycleState("DEPRECATED"));
    }

    @Transactional
    public void synchronizePipeline(com.sbase.pipeline.PipelineEntity pipeline) {
        UUID tenant=pipeline.getTenantId();
        CatalogAssetEntity pipelineAsset=upsertAsset(tenant,"PIPELINE",pipeline.getId().toString(),
            pipeline.getName(),pipeline.getOwnerSubject(),null);
        pipelineAsset.updateSourceDetails(pipeline.getName(),pipeline.getDescription(),null,null);
        pipelineAsset.setLifecycleState(pipeline.isEnabled()?"ACTIVE":"DEPRECATED");
        pipelineAsset.updateGovernance(pipeline.getDescription(),null,null,"INTERNAL",List.of(),
            Map.of("legacyPipelineId",pipeline.getId(),"version",pipeline.getVersion(),"enabled",pipeline.isEnabled()));
        assets.save(pipelineAsset);
        JsonNode definition;
        try { definition=mapper.readTree(pipeline.getDefinitionJson()); }
        catch(Exception invalidDefinition) { return; }
        JsonNode steps=definition==null?null:definition.get("steps");
        if(steps==null || !steps.isArray()) return;
        Map<String,CatalogAssetEntity> stepAssets=new LinkedHashMap<>();
        Map<String,Set<UUID>> sourceTables=new HashMap<>();
        for(JsonNode step:steps) {
            String id=firstText(step,"id"), type=firstText(step,"type");
            if(id==null || type==null || id.length()>128) continue;
            String key=pipeline.getId()+":"+id;
            CatalogAssetEntity stepAsset=upsertAsset(tenant,"PIPELINE_STEP",key,id,pipeline.getOwnerSubject(),pipelineAsset.getId());
            stepAsset.setParentId(pipelineAsset.getId());
            stepAsset.updateGovernance(null,null,null,"INTERNAL",List.of(),
                Map.of("pipelineId",pipeline.getId(),"stepId",id,"stepType",type,"pipelineVersion",pipeline.getVersion()));
            assets.save(stepAsset); stepAssets.put(id,stepAsset);
        }
        for(JsonNode step:steps) {
            String id=firstText(step,"id"), type=firstText(step,"type");
            if(!"doris_read".equals(type) || id==null) continue;
            CatalogAssetEntity source=resolveTable(tenant,firstText(step,"table"));
            if(source!=null) {
                sourceTables.put(id,Set.of(source.getId()));
                saveEdge(tenant,source.getId(),stepAssets.get(id).getId(),"READS_FROM","PIPELINE_DEFINITION",
                    pipeline.getId()+":"+id,null,BigDecimal.ONE,pipeline.getVersion(),null);
            }
        }
        for(int pass=0;pass<steps.size();pass++) {
            boolean changed=false;
            for(JsonNode step:steps) {
                String id=firstText(step,"id");
                if(id==null || stepAssets.get(id)==null) continue;
                JsonNode deps=step.get("dependsOn");
                if(deps==null || !deps.isArray()) continue;
                Set<UUID> inherited=new LinkedHashSet<>(sourceTables.getOrDefault(id,Set.of()));
                for(JsonNode dep:deps) inherited.addAll(sourceTables.getOrDefault(dep.asText(),Set.of()));
                if(!inherited.isEmpty() && !inherited.equals(sourceTables.get(id))) {
                    sourceTables.put(id,inherited); changed=true;
                }
            }
            if(!changed) break;
        }
        for(JsonNode step:steps) {
            String id=firstText(step,"id"), type=firstText(step,"type");
            if(id==null || type==null || !stepAssets.containsKey(id)) continue;
            CatalogAssetEntity target=stepAssets.get(id);
            Set<UUID> inputs=new LinkedHashSet<>();
            JsonNode deps=step.get("dependsOn");
            if(deps!=null && deps.isArray()) for(JsonNode dep:deps) {
                CatalogAssetEntity dependency=stepAssets.get(dep.asText());
                if(dependency!=null) {
                    saveEdge(tenant,dependency.getId(),target.getId(),"DEPENDS_ON","PIPELINE_DEFINITION",
                        pipeline.getId()+":"+dep.asText()+"->"+id,null,BigDecimal.ONE,pipeline.getVersion(),null);
                    inputs.addAll(sourceTables.getOrDefault(dep.asText(),Set.of()));
                }
            }
            if(inputs.isEmpty()) {
                String tableName=firstText(step,"table");
                if("doris_read".equals(type) && tableName!=null) {
                    CatalogAssetEntity source=resolveTable(tenant,tableName);
                    if(source!=null) {
                        inputs.add(source.getId());
                        saveEdge(tenant,source.getId(),target.getId(),"READS_FROM","PIPELINE_DEFINITION",
                            pipeline.getId()+":"+id,null,BigDecimal.ONE,pipeline.getVersion(),null);
                    }
                }
            } else {
                for(UUID input:inputs) saveEdge(tenant,input,target.getId(),"CONSUMES","PIPELINE_DEFINITION",
                    pipeline.getId()+":"+id,null,BigDecimal.ONE,pipeline.getVersion(),null);
            }
            sourceTables.put(id,inputs);
            for(String columnName:referencedColumns(step)) {
                for(UUID input:inputs) addColumnUse(tenant,input,columnName,target.getId(),"PIPELINE_DEFINITION",pipeline.getId()+":"+id,pipeline.getVersion());
            }
            if("doris_write".equals(type)) {
                String tableName=firstText(step,"table");
                CatalogAssetEntity output=tableName==null?null:resolveTable(tenant,tableName);
                if(output!=null) saveEdge(tenant,target.getId(),output.getId(),"WRITES_TO","PIPELINE_DEFINITION",
                    pipeline.getId()+":"+id,null,BigDecimal.ONE,pipeline.getVersion(),null);
            }
            if("feature_store_write".equals(type)) {
                String featureName=firstText(step,"featureSet");
                if(featureName!=null) assets.findAllByTenantIdAndAssetTypeOrderByUpdatedAtDesc(tenant,"FEATURE_SET",PageRequest.of(0,200))
                    .stream().filter(f->f.getDisplayName().equals(featureName)).findFirst()
                    .ifPresent(f->saveEdge(tenant,target.getId(),f.getId(),"WRITES_TO","PIPELINE_DEFINITION",
                        pipeline.getId()+":"+id,null,BigDecimal.ONE,pipeline.getVersion(),null));
            }
        }
    }

    @Transactional
    public void synchronizeReport(UUID id,UUID tenant,String name,String description,String owner,String definitionJson) {
        CatalogAssetEntity report=upsertAsset(tenant,"REPORT",id.toString(),name,owner,null);
        report.updateSourceDetails(name,description,null,null); assets.save(report);
        JsonNode definition;
        try { definition=mapper.readTree(definitionJson); } catch(Exception invalid) { return; }
        String tableName=firstText(definition,"table");
        CatalogAssetEntity table=tableName==null?null:resolveTable(tenant,tableName);
        if(table!=null) {
            saveEdge(tenant,table.getId(),report.getId(),"READS_FROM","BI_DEFINITION",id.toString(),null,
                new BigDecimal("0.950"),1,null);
            for(String columnName:reportColumns(definition)) addColumnUse(tenant,table.getId(),columnName,report.getId(),"BI_DEFINITION",id.toString(),1);
        }
    }

    @Transactional
    public void synchronizeDashboard(UUID id,UUID tenant,String name,String description,String owner) {
        CatalogAssetEntity dashboard=upsertAsset(tenant,"DASHBOARD",id.toString(),name,owner,null);
        dashboard.updateSourceDetails(name,description,null,null); assets.save(dashboard);
    }

    @Transactional
    public void synchronizeWidget(UUID id,UUID tenant,UUID dashboardId,UUID reportId,String title,String widgetType,String owner) {
        CatalogAssetEntity dashboard=assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"DASHBOARD",dashboardId.toString())
            .orElseGet(()->upsertAsset(tenant,"DASHBOARD",dashboardId.toString(),"Dashboard "+dashboardId,owner,null));
        CatalogAssetEntity widget=upsertAsset(tenant,"WIDGET",id.toString(),title,owner,dashboard.getId());
        widget.setParentId(dashboard.getId());
        widget.updateGovernance(null,null,null,"INTERNAL",List.of(),Map.of("widgetType",widgetType));
        assets.save(widget);
        saveEdge(tenant,dashboard.getId(),widget.getId(),"CONTAINS","BI_DEFINITION",id.toString(),null,BigDecimal.ONE,1,null);
        if(reportId!=null) assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"REPORT",reportId.toString())
            .ifPresent(report->saveEdge(tenant,widget.getId(),report.getId(),"USES_REPORT","BI_DEFINITION",
                id.toString(),null,BigDecimal.ONE,1,null));
    }

    private CatalogAssetEntity upsertAsset(UUID tenant,String type,String key,String name,String owner,UUID parent) {
        CatalogAssetEntity asset=assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,type,key).orElse(null);
        if(asset==null) {
            asset=new CatalogAssetEntity(tenant,type,key,name,owner);
            if(parent!=null) asset.setParentId(parent);
        }
        return asset;
    }
    private CatalogAssetEntity resolveTable(UUID tenant,String reference) {
        List<CatalogAssetEntity> matches=assets.resolveTableReference(tenant,reference,PageRequest.of(0,2));
        if(matches.size()!=1 && !reference.contains(".")) return null;
        return matches.stream().findFirst().orElse(null);
    }
    private void addColumnUse(UUID tenant,UUID tableId,String columnName,UUID consumer,String evidenceType,String evidence,long version) {
        List<CatalogColumnEntity> details=columns.findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(tenant,tableId);
        List<UUID> ids=details.stream().map(CatalogColumnEntity::getId).toList();
        assets.findAllByTenantIdAndIdIn(tenant,ids).stream()
            .filter(a->a.getDisplayName().equalsIgnoreCase(columnName)).findFirst()
            .ifPresent(a->saveEdge(tenant,a.getId(),consumer,"USES_COLUMN",evidenceType,
                evidence+":"+columnName,null,BigDecimal.ONE,version,null));
    }
    private List<String> referencedColumns(JsonNode step) {
        List<String> names=new ArrayList<>();
        for(String field:List.of("column","sourceColumn","leftKey","rightKey")) {
            String value=firstText(step,field); if(value!=null) names.add(value);
        }
        for(String field:List.of("columns","groupBy")) {
            JsonNode values=step.get(field); if(values!=null && values.isArray()) values.forEach(v->{if(v.isTextual())names.add(v.asText());});
        }
        JsonNode metrics=step.get("metrics");
        if(metrics!=null && metrics.isArray()) metrics.forEach(m->{String value=firstText(m,"column");if(value!=null)names.add(value);});
        return names.stream().distinct().toList();
    }
    private List<String> reportColumns(JsonNode definition) {
        List<String> names=new ArrayList<>();
        for(String field:List.of("dimensions")) {
            JsonNode values=definition.get(field); if(values!=null && values.isArray()) values.forEach(v->{if(v.isTextual())names.add(v.asText());});
        }
        JsonNode metrics=definition.get("metrics");
        if(metrics!=null && metrics.isArray()) metrics.forEach(m->{String value=firstText(m,"column");if(value!=null)names.add(value);});
        return names.stream().distinct().toList();
    }
    private String firstText(JsonNode node,String... fields) {
        if(node==null) return null;
        for(String field:fields) { JsonNode value=node.get(field); if(value!=null && value.isTextual() && !value.asText().isBlank()) return value.asText().trim(); }
        return null;
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
        CatalogAssetEntity column=new CatalogAssetEntity(tenant,"COLUMN",external,request.name(),table.getOwnerSubject());
        column.setParentId(tableId);
        column.updateGovernance(request.description(),request.ownerTeam()==null?table.getOwnerTeam():request.ownerTeam(),table.getDomain(),
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
        List<CatalogColumnEntity> details=columns.findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(tenant,tableId);
        Map<UUID,CatalogColumnEntity> byId=new HashMap<>(); details.forEach(col->byId.put(col.getId(),col));
        return assets.findAllByTenantIdAndIdIn(tenant,List.copyOf(byId.keySet())).stream()
            .sorted(Comparator.comparingInt(a->byId.get(a.getId()).getOrdinalPosition()))
            .map(a->view(a,byId.get(a.getId()))).toList();
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
        asset.updateGovernance(request.description()==null?asset.getDescription():trim(request.description()),
                request.ownerTeam()==null?asset.getOwnerTeam():trim(request.ownerTeam()),
                request.domain()==null?asset.getDomain():trim(request.domain()),
                request.classification()==null?asset.getClassification():classification(request.classification()),
                request.tags()==null?asset.getTags():writeTags(request.tags()),
                request.metadata()==null?asset.getMetadata():writeMetadata(request.metadata()));
        if(request.ownerSubject()!=null && !request.ownerSubject().isBlank()
                && !request.ownerSubject().trim().equals(asset.getOwnerSubject())) {
            if(request.ownerSubject().trim().length()>200) throw bad("Owner subject exceeds 200 characters");
            tenants.requireTenantAdmin(auth);
            asset.setOwnerSubject(request.ownerSubject().trim());
        }
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
            if(direction.equals("DOWNSTREAM") || direction.equals("BOTH")) batch.addAll(edges.findTop2001ByTenantIdAndFromAssetIdIn(tenant,ids));
            if(direction.equals("UPSTREAM") || direction.equals("BOTH")) batch.addAll(edges.findTop2001ByTenantIdAndToAssetIdIn(tenant,ids));
            if(batch.size()>=2001) truncated=true;
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
        if(version<1) throw bad("Effective lineage version must be positive");
        if(evidenceRef!=null && evidenceRef.length()>512) throw bad("Lineage evidence reference is too long");
        if(expression!=null && expression.length()>20000) throw bad("Lineage expression is too long");
        String fingerprint=sha256(tenant+"|"+from+"|"+to+"|"+type+"|"+evidence+"|"+Objects.toString(evidenceRef,"")+"|"+Objects.toString(expression,"")+"|"+confidence.stripTrailingZeros().toPlainString()+"|"+version);
        return edges.findByTenantIdAndEdgeFingerprint(tenant,fingerprint).orElseGet(() ->
            edges.save(new LineageEdgeEntity(tenant,from,to,type,evidence,evidenceRef,expression,confidence,version,runId,fingerprint)));
    }

    private CatalogAssetEntity requireAsset(UUID tenant,UUID id) {
        return assets.findByIdAndTenantId(id,tenant).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Catalog asset not found"));
    }
    private void requireOwnerOrAdmin(CatalogAssetEntity asset,Authentication auth) {
        boolean admin=auth.getAuthorities().stream().anyMatch(a->Set.of("ROLE_ADMIN","PERM_platform:admin").contains(a.getAuthority()));
        if(!admin && !asset.getOwnerSubject().equals(tenants.subject(auth))) tenants.requireTenantAdmin(auth);
    }
    private AssetView view(CatalogAssetEntity a,CatalogColumnEntity c) {
        return new AssetView(a.getId(),a.getAssetType(),a.getExternalKey(),a.getDisplayName(),a.getNamespace(),a.getPhysicalName(),
            a.getDescription(),a.getOwnerSubject(),a.getOwnerTeam(),a.getDomain(),a.getClassification(),a.getTags(),
            a.getMetadata(),a.getParentId(),a.getLifecycleState(),a.getSchemaFingerprint(),a.getCreatedAt(),a.getUpdatedAt(),
            c==null?null:c.getOrdinalPosition(),c==null?null:c.getDataType(),c==null?null:c.isNullable());
    }
    private EdgeView edgeView(LineageEdgeEntity e) {
        return new EdgeView(e.getId(),e.getFromAssetId(),e.getToAssetId(),e.getEdgeType(),e.getEvidenceType(),e.getEvidenceRef(),
            e.getExpressionText(),e.getConfidence(),e.getObservedAt(),e.getEffectiveVersion(),e.getRunId());
    }
    private List<String> writeTags(List<String> tags) {
        List<String> normalized=tags==null?List.of():tags.stream().filter(Objects::nonNull).map(String::trim).filter(s->!s.isBlank()).distinct().limit(50).toList();
        if(normalized.stream().anyMatch(s->s.length()>80)) throw bad("Tag exceeds 80 characters");
        return normalized;
    }
    private Map<String,Object> writeMetadata(Map<String,Object> metadata) {
        Map<String,Object> value=metadata==null?Map.of():metadata;
        String json=write(value); if(json.length()>16000) throw bad("Metadata exceeds 16 KB"); return value;
    }
    private String write(Object value) { try{return mapper.writeValueAsString(value);}catch(Exception e){throw bad("Invalid JSON metadata");} }
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
