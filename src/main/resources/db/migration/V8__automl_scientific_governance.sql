ALTER TABLE automl_experiment
  ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE model_registry
  ADD COLUMN IF NOT EXISTS model_card JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS model_monitoring_run (
  id UUID PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
  row_count INTEGER NOT NULL CHECK (row_count > 0),
  drift_level VARCHAR(16) NOT NULL CHECK (drift_level IN ('LOW','MEDIUM','HIGH')),
  report JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_model_monitoring_model_created
  ON model_monitoring_run(model_id, created_at DESC);
