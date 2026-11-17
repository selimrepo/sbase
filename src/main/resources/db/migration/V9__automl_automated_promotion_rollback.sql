CREATE TABLE IF NOT EXISTS model_promotion_event (
  id UUID PRIMARY KEY,
  alias VARCHAR(64) NOT NULL,
  previous_model_id UUID NOT NULL REFERENCES model_registry(id),
  promoted_model_id UUID NOT NULL REFERENCES model_registry(id),
  metric VARCHAR(64) NOT NULL,
  previous_score DOUBLE PRECISION NOT NULL,
  candidate_score DOUBLE PRECISION NOT NULL,
  improvement DOUBLE PRECISION NOT NULL,
  reason VARCHAR(128) NOT NULL,
  status VARCHAR(16) NOT NULL CHECK (status IN ('PROMOTED','ROLLED_BACK')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  rolled_back_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_model_promotion_alias_created
  ON model_promotion_event(alias, created_at DESC);
