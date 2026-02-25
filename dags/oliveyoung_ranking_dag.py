"""
올리브영 판매 랭킹 수집 DAG

- 스케줄: 매정시, 매 30분 (0,30분)
- 파이프라인: 크롤링(Node.js) → BigQuery 적재(Python)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
import glob
import os

PROJECT_DIR = Variable.get("oliveyoung_project_dir", default_var="/opt/airflow/app")
GCP_KEY_PATH = Variable.get("gcp_key_path", default_var="/opt/airflow/keys/target-378109-7f1aa3e0dc9a.json")
CATEGORY_LIMIT = int(Variable.get("oliveyoung_category_limit", default_var="21"))
CATEGORY_WAIT_MS = int(Variable.get("oliveyoung_category_wait_ms", default_var="2200"))

TARGET_URL = (
    "https://www.oliveyoung.co.kr/store/main/getBestList.do?"
    "t_page=2%EC%9B%94%20%EC%98%AC%EC%98%81PICK%20%EA%B8%B0%ED%9A%8D%EC%A0%84"
    "%20%EC%83%81%EC%84%B8&t_click=GNB&t_gnb_type=%EB%9E%AD%ED%82%B9&t_swiping_type=N"
)

default_args = {
    "owner": "jaeho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="oliveyoung_ranking_collector",
    default_args=default_args,
    description="올리브영 판매 랭킹 수집 및 BigQuery 적재 (30분 간격)",
    schedule_interval="0,30 * * * *",
    start_date=datetime(2026, 2, 25),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "ranking", "crawler", "bigquery"],
)


# Task 1: Playwright로 랭킹 크롤링
crawl_ranking = BashOperator(
    task_id="crawl_ranking",
    bash_command=(
        f"cd {PROJECT_DIR} && "
        f"node scripts/collect_ranking_playwright.js "
        f'--url "{TARGET_URL}" '
        f"--category-limit {CATEGORY_LIMIT} "
        f"--category-wait-ms {CATEGORY_WAIT_MS} "
        f"--out-dir output/ranking_playwright"
    ),
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)


def _load_latest_to_bigquery(**context):
    """가장 최근 수집 데이터를 BigQuery에 적재"""
    import sys
    sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH

    output_base = os.path.join(PROJECT_DIR, "output", "ranking_playwright")
    run_dirs = sorted(glob.glob(os.path.join(output_base, "*Z")))
    if not run_dirs:
        raise FileNotFoundError(f"수집 데이터 없음: {output_base}")

    latest_dir = run_dirs[-1]
    json_path = os.path.join(latest_dir, "ranking_rows.json")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON 파일 없음: {json_path}")

    from load_to_bigquery import load_to_bigquery
    row_count = load_to_bigquery(json_path)

    # XCom으로 메타데이터 전달
    context["ti"].xcom_push(key="loaded_rows", value=row_count)
    context["ti"].xcom_push(key="source_dir", value=latest_dir)
    return row_count


# Task 2: BigQuery 적재
load_to_bq = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=_load_latest_to_bigquery,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)


def _cleanup_old_runs(**context):
    """7일 이상 된 로컬 수집 데이터 정리"""
    output_base = os.path.join(PROJECT_DIR, "output", "ranking_playwright")
    cutoff = datetime.utcnow() - timedelta(days=7)

    removed = 0
    for run_dir in glob.glob(os.path.join(output_base, "*Z")):
        dir_name = os.path.basename(run_dir)
        try:
            ts = datetime.strptime(dir_name, "%Y%m%dT%H%M%SZ")
            if ts < cutoff:
                import shutil
                shutil.rmtree(run_dir)
                removed += 1
        except ValueError:
            continue

    print(f"정리 완료: {removed}개 디렉토리 삭제")
    return removed


# Task 3: 오래된 로컬 데이터 정리
cleanup = PythonOperator(
    task_id="cleanup_old_runs",
    python_callable=_cleanup_old_runs,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

# 파이프라인: 크롤링 → BigQuery 적재 → 정리
crawl_ranking >> load_to_bq >> cleanup
