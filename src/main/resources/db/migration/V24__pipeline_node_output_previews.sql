ALTER TABLE pipeline_step_runs
    ADD COLUMN IF NOT EXISTS output_preview_json TEXT NOT NULL DEFAULT '[]';

COMMENT ON COLUMN pipeline_step_runs.output_preview_json IS
    'Bounded sample of up to 20 records from node output for authorized execution inspection; never the full dataset.';
