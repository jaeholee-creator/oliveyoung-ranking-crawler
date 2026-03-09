"""올리브영 상품 식별 보강 DAG.

랭킹 fact 적재와 분리해서 신규 goods_no / retry queue만 상세 보강한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_DIR = Variable.get("oliveyoung_project_dir", default_var="/opt/airflow/app")
GCP_KEY_PATH = Variable.get("gcp_key_path", default_var="/opt/airflow/keys/target-378109-7f1aa3e0dc9a.json")
PYTHON_BIN = Variable.get("oliveyoung_python_bin", default_var="/home/ubuntu/airflow-venv/bin/python3")

IDENTITY_LOOKBACK_HOURS = int(Variable.get("oliveyoung_identity_lookback_hours", default_var="336"))
IDENTITY_LIMIT = int(Variable.get("oliveyoung_identity_limit", default_var="300"))
IDENTITY_DETAIL_WORKERS = int(Variable.get("oliveyoung_identity_detail_workers", default_var="6"))
IDENTITY_DETAIL_RETRIES = int(Variable.get("oliveyoung_identity_detail_retries", default_var="2"))
IDENTITY_DETAIL_TIMEOUT = float(Variable.get("oliveyoung_identity_detail_timeout", default_var="15"))
IDENTITY_DETAIL_PROXY_POOL = Variable.get("oliveyoung_identity_detail_proxy_pool", default_var="")
IDENTITY_RETRY_BASE_MINUTES = int(Variable.get("oliveyoung_identity_retry_base_minutes", default_var="30"))
IDENTITY_MAX_RETRY_COUNT = int(Variable.get("oliveyoung_identity_max_retry_count", default_var="12"))
IDENTITY_TIMEOUT_MINUTES = int(Variable.get("oliveyoung_identity_timeout_minutes", default_var="25"))

default_args = {
    "owner": "jaeho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="oliveyoung_product_identity_enrichment",
    default_args=default_args,
    description="올리브영 신규/retry goods_no 식별 정보 보강",
    schedule_interval="20 * * * *",
    start_date=datetime(2026, 3, 6),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "identity", "bigquery"],
)

ensure_identity_tables = BashOperator(
    task_id="ensure_identity_tables",
    bash_command=(
        f"cd {PROJECT_DIR} && "
        f"export GOOGLE_APPLICATION_CREDENTIALS={GCP_KEY_PATH} && "
        f"{PYTHON_BIN} bq/create_product_identity_tables.py --migrate"
    ),
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

enrich_identity = BashOperator(
    task_id="enrich_identity",
    bash_command=(
        f"export DISPLAY=:99 && "
        f"export PYTHONUNBUFFERED=1 && "
        f"cd {PROJECT_DIR} && "
        f"export GOOGLE_APPLICATION_CREDENTIALS={GCP_KEY_PATH} && "
        f"{PYTHON_BIN} scripts/enrich_product_identity.py "
        f"--lookback-hours {IDENTITY_LOOKBACK_HOURS} "
        f"--limit {IDENTITY_LIMIT} "
        f"--detail-workers {IDENTITY_DETAIL_WORKERS} "
        f"--detail-retries {IDENTITY_DETAIL_RETRIES} "
        f"--detail-timeout {IDENTITY_DETAIL_TIMEOUT} "
        f"--browser-profile-dir .browser_profile_identity "
        f"--retry-base-minutes {IDENTITY_RETRY_BASE_MINUTES} "
        f"--max-retry-count {IDENTITY_MAX_RETRY_COUNT}"
    ),
    env={
        "OLIVEYOUNG_DETAIL_PROXY_POOL": IDENTITY_DETAIL_PROXY_POOL,
    },
    execution_timeout=timedelta(minutes=IDENTITY_TIMEOUT_MINUTES),
    dag=dag,
)

ensure_identity_tables >> enrich_identity
