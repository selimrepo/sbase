ALTER TABLE automl_experiment DROP CONSTRAINT IF EXISTS automl_experiment_task_type_check;
ALTER TABLE automl_experiment ADD CONSTRAINT automl_experiment_task_type_check
  CHECK (task_type IN ('CLASSIFICATION','REGRESSION','TIME_SERIES'));

ALTER TABLE model_registry DROP CONSTRAINT IF EXISTS model_registry_task_type_check;
ALTER TABLE model_registry ADD CONSTRAINT model_registry_task_type_check
  CHECK (task_type IN ('CLASSIFICATION','REGRESSION','TIME_SERIES'));
