import os
import json
import subprocess  
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        # --- required ---
        "split":   Param("test",     type="string",  description="SWE-bench split, e.g. 'test'"),
        "subset":  Param("verified", type="string",  description="SWE-bench subset, e.g. 'verified'"),
        "workers": Param(5,          type="integer", minimum=1, description="Parallel workers for agent + eval"),
        # --- optional / useful ---
        "model":      Param("nebius/moonshotai/Kimi-K2.6", type="string", description="Model id served via Nebius"),
        "task_slice": Param("0:3", type="string", description="Instance slice, e.g. '0:3'"),
        "run_id":     Param("",    type=["null", "string"], description="Run id. Leave blank = auto-generate from timestamp."),
        "cost_limit": Param(0,     type="number", description="Per-instance cost limit (0 = rely on step limit)"),
    },
)
def evaluate_agent():

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def prepare_run(**context) -> dict:
        params = context["params"]

        # Resolve run_id: use provided value, else timestamp-based
        run_id = (params["run_id"] or "").strip()
        if not run_id:
            run_id = "run-" + datetime.now().strftime("%Y%m%d-%H%M%S")

        run_dir       = RUNS_ROOT / run_id
        run_agent_dir = run_dir / "run-agent"
        run_eval_dir  = run_dir / "run-eval"
        for d in (run_dir, run_agent_dir, run_eval_dir):
            d.mkdir(parents=True, exist_ok=True)

        run_config = {
            "run_id":     run_id,
            "split":      params["split"],
            "subset":     params["subset"],
            "workers":    int(params["workers"]),
            "model":      params["model"],
            "task_slice": params["task_slice"],
            "cost_limit": params["cost_limit"],
            "created_at": datetime.now().isoformat(),
        }

        config_path = run_dir / "config.json"
        config_path.write_text(json.dumps(run_config, indent=2))

        print(f"[prepare_run] run_id={run_id}")
        print(f"[prepare_run] wrote {config_path}")

        return run_config   # flows to downstream tasks via XCom

    MINI_SWE_AGENT_ROOT = PROJECT_ROOT.parent / "mini-swe-agent"
    SWEBENCH_CONFIG = MINI_SWE_AGENT_ROOT / "src/minisweagent/config/benchmarks/swebench.yaml"


    # @task(retries=3, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=30))
    # def run_agent(run_config: dict) -> str:
    #     run_id = run_config["run_id"]
    #     out_dir = RUNS_ROOT / run_id / "run-agent"
    #     out_dir.mkdir(parents=True, exist_ok=True)

    #     cmd = [
    #         "uv", "run", "mini-extra", "swebench",
    #         "--subset",  run_config["subset"],
    #         "--split",   run_config["split"],
    #         "--model",   run_config["model"],
    #         "--slice",   run_config["task_slice"],
    #         "--config",  str(SWEBENCH_CONFIG),
    #         "--workers", str(run_config["workers"]),
    #         "-o",        str(out_dir),
    #     ]
    #     print(f"[run_agent] {' '.join(cmd)}")

    #     subprocess.run(
    #         cmd,
    #         cwd=PROJECT_ROOT,
    #         check=True,
    #         env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
    #     )

    #     preds = out_dir / "preds.json"
    #     if not preds.exists():
    #         raise FileNotFoundError(f"[run_agent] expected predictions at {preds}, none found")
    #     print(f"[run_agent] predictions at {preds}")
    #     return str(preds)
    run_agent = DockerOperator(
        task_id="run_agent",
        image="mlops-agent",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        working_dir=str(PROJECT_ROOT),
        mounts=[
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
            Mount(source=str(PROJECT_ROOT), target=str(PROJECT_ROOT), type="bind"),
            Mount(source=str(MINI_SWE_AGENT_ROOT), target=str(MINI_SWE_AGENT_ROOT), type="bind"),
        ],
        environment={
            "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
            "MSWEA_COST_TRACKING": "ignore_errors",
        },
        command=[
            "uv", "run", "mini-extra", "swebench",
            "--subset", "{{ ti.xcom_pull(task_ids='prepare_run')['subset'] }}",
            "--split", "{{ ti.xcom_pull(task_ids='prepare_run')['split'] }}",
            "--model", "{{ ti.xcom_pull(task_ids='prepare_run')['model'] }}",
            "--slice", "{{ ti.xcom_pull(task_ids='prepare_run')['task_slice'] }}",
            "--config", str(SWEBENCH_CONFIG),
            "--workers", "{{ ti.xcom_pull(task_ids='prepare_run')['workers'] }}",
            "-o", str(RUNS_ROOT) + "/{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}/run-agent",
        ],
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )

    @task(retries=3, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=30))
    def run_eval(run_config: dict) -> str:
        run_id = run_config["run_id"]
        eval_dir = RUNS_ROOT / run_id / "run-eval"
        eval_dir.mkdir(parents=True, exist_ok=True)
        preds_path = RUNS_ROOT / run_id / "run-agent" / "preds.json"

        # subset 'verified' -> the Verified dataset; map here so the param drives it
        dataset = "princeton-nlp/SWE-bench_Verified"

        cmd = [
            "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name", dataset,
            "--predictions_path", str(preds_path),
            "--max_workers", str(run_config["workers"]),
            "--run_id", run_id,
        ]
        print(f"[run_eval] {' '.join(cmd)}")

        # cwd=eval_dir so SWE-bench writes its report + logs/ inside run-eval/
        subprocess.run(cmd, cwd=eval_dir, check=True, env={**os.environ})

        print(f"[run_eval] eval outputs under {eval_dir}")
        return str(eval_dir)
    
    @task(retries=2, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=5))
    def summarize_and_log(run_config: dict, eval_dir_str: str) -> dict:
        import glob
        run_id = run_config["run_id"]
        run_dir = RUNS_ROOT / run_id
        eval_dir = Path(eval_dir_str)

        candidates = glob.glob(str(eval_dir / f"*{run_id}.json"))
        if not candidates:
            raise FileNotFoundError(f"[summarize] no summary report matching *{run_id}.json in {eval_dir}")
        report_path = Path(candidates[0])
        report = json.loads(report_path.read_text())

        submitted = report.get("submitted_instances", 0)
        resolved  = report.get("resolved_instances", 0)
        # pass rate is over what we actually submitted, NOT total dataset size
        pass_rate = (resolved / submitted) if submitted else 0.0

        metrics = {
            "total_dataset_instances": report.get("total_instances", 0),  # 500 for Verified; context only
            "submitted_instances":     submitted,
            "completed_instances":     report.get("completed_instances", 0),
            "resolved_instances":      resolved,
            "unresolved_instances":    report.get("unresolved_instances", 0),
            "empty_patch_instances":   report.get("empty_patch_instances", 0),
            "error_instances":         report.get("error_instances", 0),
            "pass_rate":               round(pass_rate, 4),
        }

        metrics_path = run_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2))
        print(f"[summarize] metrics: {metrics}")
        print(f"[summarize] wrote {metrics_path}")
        # --- write manifest.json: an index of everything in this run ---
        import glob as _glob
        manifest = {
            "run_id": run_id,
            "created_at": run_config.get("created_at"),
            "config": "config.json",
            "metrics": "metrics.json",
            "agent": {
                "predictions": "run-agent/preds.json",
                "trajectories": [
                    str(Path(p).relative_to(run_dir))
                    for p in _glob.glob(str(run_dir / "run-agent" / "**" / "*.traj.json"), recursive=True)
                ],
            },
            "eval": {
                "report": str(report_path.relative_to(run_dir)),
                "logs_dir": "run-eval/logs",
            },
            "result": {
                "submitted": submitted,
                "resolved": resolved,
                "pass_rate": round(pass_rate, 4),
            },
        }
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"[summarize] wrote {manifest_path}")

        import mlflow
        # mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", str(PROJECT_ROOT / "mlruns")))
        mlflow.set_tracking_uri(
            os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///" + str(PROJECT_ROOT / "mlflow.db"))
        )
        mlflow.set_experiment("evaluate_agent")
        with mlflow.start_run(run_name=run_id):
            mlflow.log_params({
                "run_id":     run_id,
                "split":      run_config["split"],
                "subset":     run_config["subset"],
                "model":      run_config["model"],
                "task_slice": run_config["task_slice"],
                "workers":    run_config["workers"],
            })
            mlflow.log_metrics(metrics)
            mlflow.log_artifact(str(run_dir / "config.json"))
            mlflow.log_artifact(str(metrics_path))
            mlflow.log_artifact(str(manifest_path))
            mlflow.set_tag("artifact_run_dir", str(run_dir))
        print(f"[summarize] logged to MLflow at {mlflow.get_tracking_uri()}")
        return metrics
    
    cfg = prepare_run()
    eval_dir = run_eval(cfg)
    metrics = summarize_and_log(cfg, eval_dir)

    # explicit ordering: prepare_run -> run_agent (DockerOperator) -> run_eval -> summarize
    cfg >> run_agent >> eval_dir



evaluate_agent()