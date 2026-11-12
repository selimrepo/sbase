-- Extend the catalog to connector and ML assets, and connect legacy BI definitions to their sources.
INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('connector:'||c.id::text)::uuid,c.tenant_id,'CONNECTOR',c.id::text,c.name,
 NULL,c.owner_subject,jsonb_build_object('connectorType',c.connector_type,'enabled',c.enabled)
FROM ingestion_connectors c
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('registered-model:'||m.id::text)::uuid,m.tenant_id,'MODEL','registered:'||m.id::text,m.name,m.description,m.owner_subject,
 jsonb_build_object('registry','legacy','modelId',m.id)
FROM registered_models m
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('sbase-model:'||m.id::text)::uuid,m.tenant_id,'MODEL','ml:'||m.id::text,m.name,m.description,m.owner_subject,
 jsonb_build_object('registry','sbase-ml','modelId',m.id)
FROM sbase_ml_models m
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,owner_subject,parent_id,metadata_json)
SELECT md5('registered-model-version:'||v.id::text)::uuid,v.tenant_id,'MODEL','registered-version:'||v.id::text,
 m.name||' v'||v.version,v.created_by,md5('registered-model:'||m.id::text)::uuid,
 jsonb_build_object('version',v.version,'framework',v.framework,'stage',v.stage)
FROM model_versions v JOIN registered_models m ON m.id=v.model_id AND m.tenant_id=v.tenant_id
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,owner_subject,parent_id,metadata_json)
SELECT md5('sbase-model-version:'||v.id::text)::uuid,v.tenant_id,'MODEL','ml-version:'||v.id::text,
 m.name||' v'||v.version,v.created_by,md5('sbase-model:'||m.id::text)::uuid,
 jsonb_build_object('version',v.version,'framework',v.framework,'algorithm',v.algorithm,'stage',v.stage)
FROM sbase_ml_model_versions v JOIN sbase_ml_models m ON m.id=v.model_id AND m.tenant_id=v.tenant_id
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
SELECT md5('lineage:connector-dataset:'||r.id::text)::uuid,r.tenant_id,
 md5('connector:'||r.connector_id::text)::uuid,md5('dataset:'||r.dataset_id::text)::uuid,
 'WRITES_TO','RUNTIME_OBSERVED',r.id::text,1.000,md5('connector-dataset:'||r.id::text)
FROM ingestion_runs r
JOIN ingestion_connectors c ON c.id=r.connector_id AND c.tenant_id=r.tenant_id
JOIN datasets d ON d.id=r.dataset_id AND d.tenant_id=r.tenant_id
WHERE r.dataset_id IS NOT NULL
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

-- Legacy model registry versions inherit their training dataset through the experiment.
INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
SELECT md5('lineage:registered-model-training:'||v.id::text)::uuid,v.tenant_id,
 md5('dataset:'||e.dataset_id::text)::uuid,md5('registered-model-version:'||v.id::text)::uuid,
 'DERIVES_FROM','RUNTIME_OBSERVED',v.id::text,0.950,md5('registered-model-training:'||v.id::text)
FROM model_versions v JOIN experiments e ON e.id=v.experiment_id AND e.tenant_id=v.tenant_id
JOIN datasets d ON d.id=e.dataset_id AND d.tenant_id=v.tenant_id
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

-- Current ML model versions inherit their training table from the recorded run.
INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
SELECT md5('lineage:ml-model-training:'||v.id::text)::uuid,v.tenant_id,
 md5('dataset:'||d.id::text)::uuid,md5('sbase-model-version:'||v.id::text)::uuid,
 'DERIVES_FROM','RUNTIME_OBSERVED',v.id::text,0.950,md5('ml-model-training:'||v.id::text)
FROM sbase_ml_model_versions v JOIN sbase_ml_runs run ON run.id=v.run_id AND run.tenant_id=v.tenant_id
JOIN datasets d ON d.tenant_id=v.tenant_id
 AND (run.dataset_table=d.namespace||'.'||d.table_name OR run.dataset_table=d.table_name)
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

-- Resolve legacy report definitions to catalog tables and columns when their SQL contract is structured.
DO $$
DECLARE r RECORD; definition_doc JSONB; table_asset UUID; report_asset UUID; col_name TEXT; column_asset UUID;
BEGIN
 FOR r IN SELECT id,tenant_id,definition_json FROM bi_reports LOOP
   BEGIN definition_doc := r.definition_json::jsonb;
   EXCEPTION WHEN others THEN CONTINUE;
   END;
   report_asset := md5('legacy-bi-report:'||r.id::text)::uuid;
   SELECT md5('dataset:'||d.id::text)::uuid INTO table_asset FROM datasets d
    WHERE d.tenant_id=r.tenant_id
      AND (d.namespace||'.'||d.table_name=definition_doc->>'table' OR d.table_name=definition_doc->>'table')
    LIMIT 1;
   IF table_asset IS NULL THEN CONTINUE; END IF;
   INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
   VALUES(md5('lineage:legacy-report-table:'||r.id::text)::uuid,r.tenant_id,table_asset,report_asset,
     'READS_FROM','BI_DEFINITION',r.id::text,0.900,md5('legacy-report-table:'||r.id::text))
   ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
   FOR col_name IN
     SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(definition_doc->'dimensions')='array'
       THEN definition_doc->'dimensions' ELSE '[]'::jsonb END)
     UNION
     SELECT metric->>'column' FROM jsonb_array_elements(CASE WHEN jsonb_typeof(definition_doc->'metrics')='array'
       THEN definition_doc->'metrics' ELSE '[]'::jsonb END) AS metrics(metric)
       WHERE metric ? 'column'
   LOOP
     SELECT id INTO column_asset FROM catalog_assets WHERE tenant_id=r.tenant_id AND asset_type='COLUMN'
       AND parent_id=table_asset AND lower(display_name)=lower(col_name) LIMIT 1;
     IF column_asset IS NOT NULL THEN
       INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
       VALUES(md5('lineage:legacy-report-column:'||r.id::text||':'||lower(col_name))::uuid,r.tenant_id,
         column_asset,report_asset,'USES_COLUMN','BI_DEFINITION',r.id::text,0.900,
         md5('legacy-report-column:'||r.id::text||':'||lower(col_name)))
       ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
     END IF;
   END LOOP;
 END LOOP;
END $$;

-- Legacy dashboard widgets embed structured BI query definitions instead of report IDs.
DO $$
DECLARE w RECORD; definition_doc JSONB; table_asset UUID; widget_asset UUID; col_name TEXT; column_asset UUID;
BEGIN
 FOR w IN SELECT id,tenant_id,query_json FROM dashboard_widgets LOOP
   BEGIN definition_doc := w.query_json::jsonb;
   EXCEPTION WHEN others THEN CONTINUE;
   END;
   widget_asset := md5('legacy-widget:'||w.id::text)::uuid;
   SELECT md5('dataset:'||d.id::text)::uuid INTO table_asset FROM datasets d
    WHERE d.tenant_id=w.tenant_id
      AND (d.namespace||'.'||d.table_name=definition_doc->>'table' OR d.table_name=definition_doc->>'table')
    LIMIT 1;
   IF table_asset IS NULL THEN CONTINUE; END IF;
   INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
   VALUES(md5('lineage:legacy-widget-table:'||w.id::text)::uuid,w.tenant_id,table_asset,widget_asset,
     'READS_FROM','BI_DEFINITION',w.id::text,0.900,md5('legacy-widget-table:'||w.id::text))
   ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
   FOR col_name IN
     SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(definition_doc->'dimensions')='array'
       THEN definition_doc->'dimensions' ELSE '[]'::jsonb END)
     UNION
     SELECT metric->>'column' FROM jsonb_array_elements(CASE WHEN jsonb_typeof(definition_doc->'metrics')='array'
       THEN definition_doc->'metrics' ELSE '[]'::jsonb END) AS metrics(metric)
       WHERE metric ? 'column'
   LOOP
     SELECT id INTO column_asset FROM catalog_assets WHERE tenant_id=w.tenant_id AND asset_type='COLUMN'
       AND parent_id=table_asset AND lower(display_name)=lower(col_name) LIMIT 1;
     IF column_asset IS NOT NULL THEN
       INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
       VALUES(md5('lineage:legacy-widget-column:'||w.id::text||':'||lower(col_name))::uuid,w.tenant_id,
         column_asset,widget_asset,'USES_COLUMN','BI_DEFINITION',w.id::text,0.900,
         md5('legacy-widget-column:'||w.id::text||':'||lower(col_name)))
       ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
     END IF;
   END LOOP;
 END LOOP;
END $$;
