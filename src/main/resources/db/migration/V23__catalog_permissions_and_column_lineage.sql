-- Grant catalog/lineage capabilities to the built-in platform roles.
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r
CROSS JOIN (VALUES ('catalog:read'),('catalog:write'),('lineage:read'),('lineage:write')) p(permission)
WHERE r.name IN ('ADMIN','DATA_ENGINEER')
ON CONFLICT DO NOTHING;

INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r
CROSS JOIN (VALUES ('catalog:read'),('lineage:read')) p(permission)
WHERE r.name IN ('DATA_SCIENTIST','BI_VIEWER')
ON CONFLICT DO NOTHING;

-- Ordinal changes are applied as a set by schema discovery; defer uniqueness until transaction commit.
ALTER TABLE catalog_columns DROP CONSTRAINT IF EXISTS uq_catalog_column_ordinal;
ALTER TABLE catalog_columns ADD CONSTRAINT uq_catalog_column_ordinal
  UNIQUE (tenant_id,table_asset_id,ordinal_position) DEFERRABLE INITIALLY DEFERRED;

-- Re-run pipeline dependency materialization after all step nodes exist (handles forward references).
DO $$
DECLARE p RECORD; step JSONB; dep TEXT; step_id TEXT; dep_asset UUID; step_asset UUID;
BEGIN
 FOR p IN SELECT id,tenant_id,definition_json FROM pipelines LOOP
   BEGIN
     FOR step IN SELECT value FROM jsonb_array_elements((p.definition_json::jsonb)->'steps') LOOP
       step_id := step->>'id';
       IF step_id IS NULL OR jsonb_typeof(step->'dependsOn') <> 'array' THEN CONTINUE; END IF;
       step_asset := md5('pipeline-step:'||p.id::text||':'||step_id)::uuid;
       FOR dep IN SELECT jsonb_array_elements_text(step->'dependsOn') LOOP
         dep_asset := md5('pipeline-step:'||p.id::text||':'||dep)::uuid;
         INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
         SELECT md5('lineage:pipeline-dependency:'||p.id::text||':'||dep||':'||step_id)::uuid,p.tenant_id,
           dep_asset,step_asset,'DEPENDS_ON','PIPELINE_DEFINITION',dep||' -> '||step_id,
           md5('pipeline-dependency:'||p.id::text||':'||dep||':'||step_id)
         WHERE EXISTS (SELECT 1 FROM catalog_assets a WHERE a.id=dep_asset AND a.tenant_id=p.tenant_id)
           AND EXISTS (SELECT 1 FROM catalog_assets a WHERE a.id=step_asset AND a.tenant_id=p.tenant_id)
         ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
       END LOOP;
     END LOOP;
   EXCEPTION WHEN others THEN
     -- Malformed legacy pipeline JSON remains visible as a pipeline asset with partial lineage.
     CONTINUE;
   END;
 END LOOP;
END $$;

-- Column-to-feature lineage from existing feature transformation declarations.
DO $$
DECLARE l RECORD; source_col TEXT; schema_doc JSONB; source_asset UUID; feature_asset UUID; column_asset UUID;
BEGIN
 FOR l IN
   SELECT fl.id,fl.tenant_id,fl.source_columns_json,fl.transformation_json,fl.source_dataset_id,fl.feature_set_id
   FROM feature_lineage fl WHERE fl.source_dataset_id IS NOT NULL
 LOOP
   source_asset := md5('dataset:'||l.source_dataset_id::text)::uuid;
   feature_asset := md5('feature-set:'||l.feature_set_id::text)::uuid;
   BEGIN schema_doc := l.source_columns_json::jsonb;
   EXCEPTION WHEN others THEN CONTINUE;
   END;
   IF jsonb_typeof(schema_doc) <> 'array' THEN CONTINUE; END IF;
   FOR source_col IN SELECT jsonb_array_elements_text(schema_doc) LOOP
     SELECT id INTO column_asset FROM catalog_assets
       WHERE tenant_id=l.tenant_id AND asset_type='COLUMN'
         AND external_key=l.source_dataset_id::text||':'||lower(source_col)
       LIMIT 1;
     IF column_asset IS NOT NULL THEN
       INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,
         expression_text,confidence,edge_fingerprint)
       VALUES(md5('lineage:feature-column:'||l.id::text||':'||lower(source_col))::uuid,l.tenant_id,
         column_asset,feature_asset,'USES_COLUMN','USER_DECLARED',l.id::text,l.transformation_json,0.950,
         md5('feature-column:'||l.id::text||':'||lower(source_col)))
       ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
     END IF;
   END LOOP;
 END LOOP;
END $$;

-- Column lineage for saved BI reports. Unknown/unregistered columns are intentionally left unresolved.
DO $$
DECLARE r RECORD; definition_doc JSONB; table_asset UUID; col_name TEXT; column_asset UUID; report_asset UUID;
BEGIN
 FOR r IN SELECT id,tenant_id,definition_json FROM sbase_bi_reports LOOP
   definition_doc := r.definition_json;
   report_asset := md5('bi-report:'||r.id::text)::uuid;
   SELECT a.id INTO table_asset
   FROM catalog_assets a JOIN datasets d ON a.id=md5('dataset:'||d.id::text)::uuid
   WHERE d.tenant_id=r.tenant_id
     AND (d.namespace||'.'||d.table_name=definition_doc->>'table' OR d.table_name=definition_doc->>'table')
   LIMIT 1;
   IF table_asset IS NULL THEN CONTINUE; END IF;
   FOR col_name IN
     SELECT jsonb_array_elements_text(COALESCE(definition_doc->'dimensions','[]'::jsonb))
     UNION
     SELECT metric->>'column' FROM jsonb_array_elements(COALESCE(definition_doc->'metrics','[]'::jsonb)) metric
       WHERE metric ? 'column'
   LOOP
     SELECT a.id INTO column_asset FROM catalog_assets a
     WHERE a.tenant_id=r.tenant_id AND a.asset_type='COLUMN'
       AND a.parent_id=table_asset AND lower(a.display_name)=lower(col_name)
     LIMIT 1;
     IF column_asset IS NOT NULL THEN
       INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,confidence,edge_fingerprint)
       VALUES(md5('lineage:bi-column:'||r.id::text||':'||lower(col_name))::uuid,r.tenant_id,
         column_asset,report_asset,'USES_COLUMN','BI_DEFINITION',r.id::text,0.950,
         md5('bi-column:'||r.id::text||':'||lower(col_name)))
       ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
     END IF;
   END LOOP;
 END LOOP;
END $$;
