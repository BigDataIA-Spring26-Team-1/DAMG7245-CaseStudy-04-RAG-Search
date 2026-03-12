from __future__ import annotations

import importlib
from datetime import datetime, timedelta
from pathlib import Path

try:
    airflow_module = importlib.import_module("airflow")
    airflow_bash_operators = importlib.import_module("airflow.operators.bash")
    DAG = airflow_module.DAG
    BashOperator = airflow_bash_operators.BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    DAG = None
    BashOperator = None
    AIRFLOW_AVAILABLE = False

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT
SCRIPTS_DIR = APP_ROOT / "scripts"

default_args = {
    "owner": "org-air-cs4",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="evidence_indexing_pipeline",
        default_args=default_args,
        description="CS4 pipeline: refresh evidence and rebuild retrieval index",
        schedule="0 6 * * *",
        start_date=datetime(2026, 3, 1),
        catchup=False,
        tags=["cs4", "rag", "indexing", "org-air"],
    ) as dag:

        export_env = """
        export PYTHONPATH="${PYTHONPATH}:."
        """

        index_evidence = BashOperator(
            task_id="index_evidence",
            bash_command=f"""
            cd {APP_ROOT}
            {export_env}
            poetry run python {SCRIPTS_DIR / "index_evidence.py"}
            """,
        )

        compute_scoring = BashOperator(
            task_id="compute_scoring",
            bash_command=f"""
            cd {APP_ROOT}
            {export_env}
            if [ -n "$COMPANY_ID" ]; then
              poetry run python {SCRIPTS_DIR / "run_scoring_engine.py"} --company-id "$COMPANY_ID" --version v1.0
            else
              echo "COMPANY_ID not set; skipping scoring step"
            fi
            """,
        )

        smoke_validate = BashOperator(
            task_id="smoke_validate",
            bash_command=f"""
            cd {APP_ROOT}
            {export_env}
            poetry run python -m py_compile app/services/retrieval/hybrid.py
            poetry run python -m py_compile app/services/justification/generator.py
            poetry run python -m py_compile app/services/workflows/ic_prep.py
            poetry run python -m py_compile app/services/workflows/analyst_notes.py
            poetry run python -m py_compile app/services/llm/router.py
            """,
        )

        index_evidence >> compute_scoring >> smoke_validate
else:
    dag = None
