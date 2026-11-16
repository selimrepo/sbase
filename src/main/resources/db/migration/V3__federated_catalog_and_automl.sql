CREATE TABLE IF NOT EXISTS external_catalog_source (
  id UUID PRIMARY KEY,
  source_name VARCHAR(120) NOT NULL UNIQUE,
  source_type VARCHAR(32) NOT NULL CHECK (source_type IN ('POSTGRESQL','CASSANDRA','SOLR')),
  endpoint TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  options JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS catalog_dataset (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL REFERENCES external_catalog_source(id) ON DELETE CASCADE,
  namespace VARCHAR(255) NOT NULL,
  dataset_name VARCHAR(255) NOT NULL,
  schema_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_count_estimate BIGINT,
  bytes_estimate BIGINT,
  statistics_updated_at TIMESTAMPTZ,
  UNIQUE(source_id, namespace, dataset_name)
);
CREATE INDEX IF NOT EXISTS ix_catalog_dataset_source ON catalog_dataset(source_id);
CREATE TABLE IF NOT EXISTS federated_query_execution (
  id UUID PRIMARY KEY,
  query_text TEXT NOT NULL,
  logical_plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  physical_plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  status VARCHAR(24) NOT NULL CHECK(status IN ('PLANNED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  estimated_cost DOUBLE PRECISION,
  actual_rows BIGINT,
  elapsed_ms BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS automl_experiment (
  id UUID PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  task_type VARCHAR(32) NOT NULL CHECK(task_type IN ('CLASSIFICATION','REGRESSION')),
  label_column VARCHAR(255) NOT NULL,
  feature_columns JSONB NOT NULL,
  status VARCHAR(24) NOT NULL CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  best_algorithm VARCHAR(120),
  best_metric VARCHAR(80),
  best_score DOUBLE PRECISION,
  model_uri TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS automl_trial (
  experiment_id UUID NOT NULL REFERENCES automl_experiment(id) ON DELETE CASCADE,
  trial_number INTEGER NOT NULL,
  algorithm VARCHAR(120) NOT NULL,
  parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  status VARCHAR(24) NOT NULL,
  duration_ms BIGINT,
  PRIMARY KEY(experiment_id, trial_number)
);
CREATE INDEX IF NOT EXISTS ix_automl_experiment_status ON automl_experiment(status, created_at DESC);
