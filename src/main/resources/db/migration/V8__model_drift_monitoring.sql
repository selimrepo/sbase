CREATE TABLE IF NOT EXISTS model_monitoring_run (
  id UUID PRIMARY KEY,
  model_id UUID REFERENCES model_registry(id) ON DELETE SET NULL,
  reference_rows INTEGER NOT NULL CHECK (reference_rows > 0),
  current_rows INTEGER NOT NULL CHECK (current_rows > 0),
  psi_threshold DOUBLE PRECISION NOT NULL CHECK (psi_threshold > 0),
  status VARCHAR(24) NOT NULL CHECK (status IN ('STABLE','DRIFT_DETECTED')),
  report JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_model_monitoring_model_created
  ON model_monitoring_run(model_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_model_monitoring_status_created
  ON model_monitoring_run(status, created_at DESC);
