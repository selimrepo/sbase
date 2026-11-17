ALTER TABLE automl_experiment
  ADD COLUMN IF NOT EXISTS evaluation_report JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS reproducibility JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS training_config JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE model_registry
  ADD COLUMN IF NOT EXISTS evaluation_report JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS reproducibility JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS training_config JSONB NOT NULL DEFAULT '{}'::jsonb;
