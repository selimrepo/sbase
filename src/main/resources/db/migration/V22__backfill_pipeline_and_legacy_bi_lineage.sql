-- Connect existing pipeline step definitions and both BI workspace generations to the unified graph.
INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('legacy-bi-report:'||r.id::text)::uuid,r.tenant_id,'REPORT','legacy:'||r.id::text,r.name,r.description,r.owner_subject,
 jsonb_build_object('legacyReportId',r.id,'workspace','legacy')
FROM bi_reports r
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('legacy-dashboard:'||d.id::text)::uuid,d.tenant_id,'DASHBOARD','legacy:'||d.id::text,d.name,d.description,d.owner_subject,
 jsonb_build_object('legacyDashboardId',d.id,'workspace','legacy')
FROM dashboards d
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,owner_subject,parent_id,metadata_json)
SELECT md5('legacy-widget:'||w.id::text)::uuid,d.tenant_id,'WIDGET','legacy:'||w.id::text,w.title,d.owner_subject,
 md5('legacy-dashboard:'||d.id::text)::uuid,jsonb_build_object('legacyWidgetId',w.id,'widgetType',w.widget_type)
FROM dashboard_widgets w JOIN dashboards d ON d.id=w.dashboard_id
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
SELECT md5('lineage:legacy-dashboard-widget:'||w.id::text)::uuid,d.tenant_id,
 md5('legacy-dashboard:'||d.id::text)::uuid,md5('legacy-widget:'||w.id::text)::uuid,
 'CONTAINS','MIGRATION',w.id::text,md5('legacy-dashboard-widget:'||w.id::text)
FROM dashboard_widgets w JOIN dashboards d ON d.id=w.dashboard_id
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

-- SQL-backed BI report definitions point from the governed physical table to the report asset.
INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
SELECT md5('lineage:bi-report-table:'||r.id::text||':'||d.id::text)::uuid,r.tenant_id,
 md5('dataset:'||d.id::text)::uuid,md5('bi-report:'||r.id::text)::uuid,
 'READS_FROM','BI_DEFINITION',r.id::text,0.900,md5('bi-report-table:'||r.id::text||':'||d.id::text)
FROM sbase_bi_reports r
JOIN LATERAL (SELECT r.definition_json->>'table' AS table_name) q ON q.table_name IS NOT NULL
JOIN datasets d ON d.tenant_id=r.tenant_id
 AND (q.table_name=d.namespace||'.'||d.table_name OR q.table_name=d.table_name)
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

DO $$
DECLARE p RECORD; step JSONB; dep TEXT; step_id TEXT; step_type TEXT; step_asset UUID;
        dep_asset UUID; table_asset UUID; table_name TEXT; definition_doc JSONB;
BEGIN
 FOR p IN SELECT id,tenant_id,owner_subject,definition_json FROM pipelines LOOP
   BEGIN definition_doc := p.definition_json::jsonb;
   EXCEPTION WHEN others THEN CONTINUE;
   END;
   IF jsonb_typeof(definition_doc->'steps') <> 'array' THEN CONTINUE; END IF;
   FOR step IN SELECT value FROM jsonb_array_elements(definition_doc->'steps') LOOP
     step_id := step->>'id'; step_type := step->>'type';
     IF step_id IS NULL OR step_type IS NULL THEN CONTINUE; END IF;
     step_asset := md5('pipeline-step:'||p.id::text||':'||step_id)::uuid;
     INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,owner_subject,parent_id,metadata_json)
     VALUES(step_asset,p.tenant_id,'PIPELINE_STEP',p.id::text||':'||step_id,step_id,p.owner_subject,
       md5('pipeline:'||p.id::text)::uuid,jsonb_build_object('pipelineId',p.id,'stepId',step_id,'stepType',step_type))
     ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

     INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
     VALUES(md5('lineage:pipeline-step:'||p.id::text||':'||step_id)::uuid,p.tenant_id,
       md5('pipeline:'||p.id::text)::uuid,step_asset,'CONTAINS','PIPELINE_DEFINITION',step_id,
       md5('pipeline-step:'||p.id::text||':'||step_id))
     ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

     IF jsonb_typeof(step->'dependsOn')='array' THEN
       FOR dep IN SELECT jsonb_array_elements_text(step->'dependsOn') LOOP
         dep_asset := md5('pipeline-step:'||p.id::text||':'||dep)::uuid;
         INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
         SELECT md5('lineage:pipeline-dependency:'||p.id::text||':'||dep||':'||step_id)::uuid,p.tenant_id,
           dep_asset,step_asset,'DEPENDS_ON','PIPELINE_DEFINITION',dep||' -> '||step_id,
           md5('pipeline-dependency:'||p.id::text||':'||dep||':'||step_id)
         WHERE EXISTS (SELECT 1 FROM catalog_assets a WHERE a.id=dep_asset AND a.tenant_id=p.tenant_id)
         ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
       END LOOP;
     END IF;

     IF step_type='doris_read' THEN
       table_name := step->>'table';
       SELECT a.id INTO table_asset FROM catalog_assets a
       JOIN datasets d ON a.id=md5('dataset:'||d.id::text)::uuid
       WHERE d.tenant_id=p.tenant_id AND (d.namespace||'.'||d.table_name=table_name OR d.table_name=table_name)
       LIMIT 1;
       IF table_asset IS NOT NULL THEN
         INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
         VALUES(md5('lineage:pipeline-read:'||p.id::text||':'||step_id)::uuid,p.tenant_id,table_asset,step_asset,
           'READS_FROM','PIPELINE_DEFINITION',table_name,md5('pipeline-read:'||p.id::text||':'||step_id))
         ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
       END IF;
     ELSIF step_type='doris_write' THEN
       table_name := step->>'table';
       SELECT a.id INTO table_asset FROM catalog_assets a
       JOIN datasets d ON a.id=md5('dataset:'||d.id::text)::uuid
       WHERE d.tenant_id=p.tenant_id AND (d.namespace||'.'||d.table_name=table_name OR d.table_name=table_name)
       LIMIT 1;
       IF table_asset IS NOT NULL THEN
         INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
         VALUES(md5('lineage:pipeline-write:'||p.id::text||':'||step_id)::uuid,p.tenant_id,step_asset,table_asset,
           'WRITES_TO','PIPELINE_DEFINITION',table_name,md5('pipeline-write:'||p.id::text||':'||step_id))
         ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
       END IF;
     END IF;
   END LOOP;
 END LOOP;
END $$;
