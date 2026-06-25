# REPORT — Evaluation pipeline for coding-agent experiments

**Author:** Hila Roffman
**Assignment:** Nebius Academy AI Performance Engineering - MLOps module, Lecture 6 (End-to-end ML pipeline)

## 1. Overview

This project turns the ad-hoc `mini-swe-agent` + SWE-bench evaluation scripts into a
configurable, reproducible Airflow pipeline that runs the `run-agent -> run-evaluation`
workflow and tracks every run in MLflow.

The pipeline runs `mini-swe-agent` on a slice of SWE-bench_Verified instances, evaluates
the produced patches with the SWE-bench harness, and records parameters, metrics, and
artifact references so multiple experiments can be compared.

## 2. Architecture

DAG `evaluate_agent` (in `dags/evaluate_agent.py`), four sequential tasks:
prepare_run -> run_agent -> run_eval -> summarize_and_log

- **prepare_run** — reads Airflow params, resolves a `run_id` (auto-generated from a
  timestamp if not provided), and writes `runs/<run-id>/config.json`.
- **run_agent** — runs `mini-extra swebench` with the params, writing trajectories and
  `preds.json` into `runs/<run-id>/run-agent/`.
- **run_eval** — runs `swebench.harness.run_evaluation` on that `preds.json`, writing the
  summary report and per-instance logs into `runs/<run-id>/run-eval/`.
- **summarize_and_log** — parses the eval report into `runs/<run-id>/metrics.json`, writes
  a `manifest.json` indexing all artifacts, and logs params + metrics + artifacts to MLflow.

Each task passes its output to the next via XCom (e.g. `run_agent` returns the `preds.json`
path consumed by `run_eval`), so the dependency graph reflects the actual data flow.

## 3. Configuration (Airflow params)

The DAG is fully parameterized — no hard-coded experiment values:

| Param | Required | Default | Purpose |
|---|---|---|---|
| `split` | yes | `test` | SWE-bench split |
| `subset` | yes | `verified` | SWE-bench subset |
| `workers` | yes | `5` | parallel workers (agent + eval) |
| `model` | no | `nebius/moonshotai/Kimi-K2.6` | model served via Nebius |
| `task_slice` | no | `0:3` | instance slice |
| `run_id` | no | (blank → auto timestamp) | run identifier |
| `cost_limit` | no | `0` | per-instance cost limit |

## 4. How to run

```bash
# 1. Start Airflow (standalone) — includes mlflow in the runtime
bash run-airflow-standalone.sh
# forward port 8080, open http://localhost:8080  (admin / admin)

# 2. In the UI: trigger the `evaluate_agent` DAG, adjust params if desired, Trigger.

# 3. View results in MLflow:
uv run mlflow ui --backend-store-uri "sqlite:///$PWD/mlflow.db" --port 5000
# forward port 5000, open http://localhost:5000  (switch to "Model training" view)
```

## 5. Artifact layout

Each run produces a self-contained, reproducible folder:
Each run produces a self-contained, reproducible folder:
runs/<run-id>/

├── config.json        # resolved params for this run

├── manifest.json      # index of all artifacts + result summary

├── metrics.json       # pass rate and instance counts

├── run-agent/

│   ├── preds.json

│   └── <instance>/<instance>.traj.json   # agent trajectories

└── run-eval/

├── <model>.<run-id>.json             # SWE-bench summary report

└── logs/run_evaluation/...           # per-instance test logs
`manifest.json` points at every important file, so the run can be handed off as one folder
and fully reconstructed.

## 6. MLflow tracking

Runs are logged to a local SQLite-backed MLflow store (`mlflow.db`), experiment
`evaluate_agent`. Each run records params (model, split, subset, task_slice, workers),
metrics, and the run-folder artifacts. See `screenshots/mlflow_runs.png`.

The tracking URI reads `MLFLOW_TRACKING_URI` from the environment if set, falling back to
the local SQLite store — so pointing at a remote MLflow server (Phase 3) requires no code
change.

## 7. Example completed run

Run `run-20260625-170546`, model `nebius/moonshotai/Kimi-K2.6`, slice `0:3`:

| Metric | Value |
|---|---|
| submitted_instances | 3 |
| resolved_instances | 2 |
| pass_rate | 0.6667 |

Note: SWE-bench's report lists `total_instances: 500` (the full Verified set). The
meaningful pass rate is `resolved / submitted` (2/3), not `resolved / total`.

## 8. Rerun by run-id

Trigger the DAG with `run_id=<id>` to reproduce a known run into `runs/<id>/`. With a blank
`run_id`, a fresh timestamped id is generated. `config.json` + `manifest.json` capture
everything needed to understand or rerun an experiment.

## 9. Remote storage (S3 / Object Storage)

Not uploaded in this iteration. Each run is written as a complete local `runs/<run-id>/`
tree. To move to Object Storage, a `log-artifacts-to-s3` task would tar `runs/<run-id>/`
and upload it to a Nebius Object Storage bucket (S3-compatible) via `boto3`, then log the
resulting `s3://.../<run-id>.tar.gz` URI to MLflow as an artifact reference. The
`manifest.json` already records relative artifact paths, so it remains valid inside the
archive.

## 10. What I'd do with more time

- Replace direct subprocess calls with `DockerOperator` using the provided `Dockerfile`
  for isolated, repeatable execution.
- Deploy Airflow + MLflow via `docker-compose` instead of standalone.
- Add retries/timeouts on the agent, eval, and logging tasks.
- Upload run folders to Object Storage and log the URIs to MLflow.