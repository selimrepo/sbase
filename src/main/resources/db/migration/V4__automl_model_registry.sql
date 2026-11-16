CREATE TABLE IF NOT EXISTS model_registry (
  id UUID PRIMARY KEY,
  experiment_id UUID NOT NULL REFERENCES automl_experiment(id),
  name VARCHAR(200) NOT NULL,
  algorithm VARCHAR(120) NOT NULL,
  task_type VARCHAR(32) NOT NULL CHECK(task_type IN ('CLASSIFICATION','REGRESSION')),
  metric VARCHAR(80) NOT NULL,
  best_score DOUBLE PRECISION NOT NULL,
  model_uri TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  status VARCHAR(24) NOT NULL CHECK(status IN ('READY','ARCHIVED','FAILED')),
  feature_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(experiment_id, version)
);
CREATE INDEX IF NOT EXISTS ix_model_registry_created ON model_registry(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_model_registry_status ON model_registry(status);
ALTER TABLE automl_experiment
  ADD COLUMN IF NOT EXISTS duration_ms BIGINT,
  ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE automl_trial
  ADD COLUMN IF NOT EXISTS error_message TEXT;
