CREATE TABLE IF NOT EXISTS model_alias (
  alias VARCHAR(64) PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES model_registry(id),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_model_alias_model ON model_alias(model_id);
