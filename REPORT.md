# REPORT — Evaluation pipeline for coding-agent experiments

**Author:** Hila Roffman
**Assignment:** Nebius Academy AI Performance Engineering — MLOps module, Lecture 6 (End-to-end ML pipeline)

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
- **run_agent** — runs `mini-extra swebench` inside a container (DockerOperator, see §10),
  writing trajectories and `preds.json` into `runs/<run-id>/run-agent/`.
- **run_eval** — runs `swebench.harness.run_evaluation` on that `preds.json`, writing the
  summary report and per-instance logs into `runs/<run-id>/run-eval/`.
- **summarize_and_log** — parses the eval report into `runs/<run-id>/metrics.json`, writes
  a `manifest.json` indexing all artifacts, and logs params + metrics + artifacts to MLflow.

Tasks pass state via XCom, so the dependency graph reflects the actual data flow. All tasks
carry retries and execution timeouts so transient failures (e.g. HuggingFace 504s) are
retried rather than failing the run.

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
# 1. Start Airflow (standalone) from a shell with docker group access
#    (the eval step and the DockerOperator both need the Docker socket):
newgrp docker
bash run-airflow-standalone.sh
# forward port 8080, open http://localhost:8080  (admin / admin)

# 2. Build the agent image once (used by the run_agent DockerOperator):
docker build -t mlops-agent .

# 3. In the UI: trigger the `evaluate_agent` DAG, adjust params if desired, Trigger.

# 4. View results in MLflow:
uv run mlflow ui --backend-store-uri "sqlite:///$PWD/mlflow.db" --port 5000
# forward port 5000, open http://localhost:5000  (switch to "Model training" view)
```

## 5. Artifact layout

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
the local SQLite store — so pointing at a remote MLflow server requires no code change.

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

## 9. Reliability & operational notes

- **Retries/timeouts:** network-facing tasks (`run_agent`, `run_eval`) retry up to 3× with
  a delay; observed a transient HuggingFace `504` and a Docker-socket permission race during
  development — retries make these self-healing.
- **Docker access:** Airflow must be launched from a shell with `docker` group membership,
  since `run_eval` (and the DockerOperator) talk to the Docker daemon.

## 10. Containerized execution (DockerOperator) — status & limitation

`run_agent` runs via **DockerOperator** using the provided `Dockerfile`, with the Docker
socket bind-mounted so the agent spawns its own per-instance containers (Docker-in-Docker).
This works end-to-end — `run_agent` completes successfully as a DockerOperator task (see
`screenshots/dockeroperator_run_agent.png`).

**Known limitation:** the agent container runs as root and writes into the bind-mounted
project directory, creating root-owned files that then block the subprocess-based `run_eval`
(running as the host user) from reading `.venv`. Running the container as the host user
(`user=UID:GID`) instead surfaces a second layer — `uv` and the agent expect writable
`HOME`/`/.cache`, which root owns in the image. The correct production fix is a dedicated,
correctly-permissioned **artifacts volume** (rather than a project bind mount) shared between
containerized agent and eval steps, plus a non-root user baked into the image. Given time
constraints, `run_eval` remains a subprocess task; the DockerOperator pattern is demonstrated
on `run_agent`.

## 11. What I'd do with more time
- Run both `run_agent` and `run_eval` as DockerOperators over a shared artifacts volume with
  correct ownership (resolving the limitation in §10).
- Deploy Airflow + MLflow via `docker-compose` instead of standalone.
- Upload run folders to Object Storage (S3) and log the URIs to MLflow; `manifest.json`
  already uses relative paths so it stays valid inside an uploaded archive.