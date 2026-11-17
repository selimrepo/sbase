# AutoML temporal validation and drift automation

## Rolling-origin cross-validation

For `task=time_series`, AutoML now performs expanding-window walk-forward validation inside the chronological 80/20 training/holdout split:

- `rollingFolds` controls the number of folds (2–5, default 3).
- Each fold trains only on observations earlier than its validation window.
- The training window expands after every fold; validation windows do not overlap.
- Preprocessing estimators (median imputation, categorical indexing/encoding and vector assembly) are fitted independently inside each fold.
- Numeric/categorical constant-feature selection is based on the chronological training window, not the future holdout.
- Candidate selection uses the mean configured metric across rolling folds. The final model is refitted on the complete training window and reported once against the untouched chronological holdout.
- Lagged features use walk-forward one-step semantics: later rows may use already observed earlier target values. This is not a multi-horizon forecast benchmark.

Example training request:

```json
{
  "task": "time_series",
  "timeColumn": "observedAt",
  "label": "demand",
  "features": ["price", "promotion"],
  "lags": 6,
  "rollingFolds": 4,
  "optimizationMetric": "rmse",
  "data": []
}
```

Replace `data` with the labeled observations; at least 50 usable rows are required after lag generation.

## Drift-triggered retraining

`POST /api/v1/models/{modelId}/monitoring` continues to accept `rows` for feature drift analysis. Optional automation is configured in `policy`. Retraining requires a separate, labeled `retrainingRequest`; unlabeled drift batches are never silently treated as training data.

```json
{
  "rows": [],
  "policy": {
    "autoRetrain": true,
    "triggerLevel": "HIGH",
    "cooldownHours": 24,
    "autoPromote": true,
    "promotionAlias": "production",
    "minimumImprovement": 0.02,
    "promotionEvaluationData": []
  },
  "retrainingRequest": {
    "name": "drift-retrain-2026-11",
    "task": "regression",
    "label": "target",
    "features": ["featureA", "featureB"],
    "optimizationMetric": "rmse",
    "data": []
  }
}
```

Replace all empty arrays with actual rows. `rows` may be unlabeled and is used for drift detection. `retrainingRequest.data` must contain at least 50 labeled rows and match the monitored model's task. `promotionEvaluationData` must be a separate, labeled holdout (2–50,000 rows) with the same label and feature schema; it is used to score incumbent and candidate on exactly the same observations. Do not reuse training rows as the promotion holdout.

Policy fields:

- `autoRetrain`: defaults to false.
- `triggerLevel`: `MEDIUM` or `HIGH` threshold; defaults to `HIGH`.
- `cooldownHours`: prevents repeated successful retraining for the same source model (0–720, default 24).
- `autoPromote`: defaults to false. If enabled, `promotionAlias` is required.
- `minimumImprovement`: minimum absolute metric improvement on the common evaluation set; defaults to 0.0. The metric direction is read from the model card.
- `promotionEvaluationData`: required for automatic promotion. Candidate and incumbent feature schemas and optimization metrics must match.

An automatic promotion occurs only if the requested alias currently points to the monitored model, the candidate is READY, both models can be scored on the common labeled holdout, and the candidate improves by at least the configured margin. A changed alias, mismatched schema/metric, missing evaluation set, or insufficient improvement blocks promotion. Retraining may succeed while promotion is blocked; the candidate remains in the registry for review.

## Rollback

Successful automatic promotions are recorded in `model_promotion_event`. Restore the previous model with:

```http
POST /api/v1/models/aliases/production/rollback
```

Rollback is conditional: it succeeds only if the alias still points to the model from the latest recorded promotion and the previous model remains READY. This prevents an old rollback event from overwriting a newer manual deployment.

## Operational notes

- Monitoring history stores both the drift report and the automation decision.
- A drift threshold is a trigger, not proof of model degradation. Retraining and promotion are separate decisions.
- Promotion evaluation is a common holdout comparison, not a statistical significance test. For high-impact models, add repeated temporal holdouts and domain-specific acceptance thresholds.
- For time-series models, provide a chronologically representative labeled evaluation holdout with all lag features required by the model.
