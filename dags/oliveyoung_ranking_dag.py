"""
올리브영 판매 랭킹 수집 DAG

- 스케줄: 매시 30분
- 파이프라인: 크롤링(patchright) → BigQuery 적재(Python) → 정리 → Slack 알림
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
import glob
import os
import urllib.request
import urllib.error

PROJECT_DIR = Variable.get("oliveyoung_project_dir", default_var="/opt/airflow/app")
GCP_KEY_PATH = Variable.get("gcp_key_path", default_var="/opt/airflow/keys/target-378109-7f1aa3e0dc9a.json")
CATEGORY_LIMIT = int(Variable.get("oliveyoung_category_limit", default_var="21"))
CATEGORY_WAIT_MS = int(Variable.get("oliveyoung_category_wait_ms", default_var="2200"))
SLACK_BOT_TOKEN = Variable.get("slack_bot_token", default_var="")
SLACK_CHANNEL_ID = Variable.get("slack_channel_id", default_var="C0ACH02BLG5")
PYTHON_BIN = Variable.get("oliveyoung_python_bin", default_var="/home/ubuntu/airflow-venv/bin/python3")

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
    description="올리브영 판매 랭킹 수집 및 BigQuery 적재 (매시 30분)",
    schedule_interval="30 * * * *",
    start_date=datetime(2026, 2, 25),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "ranking", "crawler", "bigquery"],
)


# Task 1: patchright(Scrapling)로 랭킹 크롤링
crawl_ranking = BashOperator(
    task_id="crawl_ranking",
    bash_command=(
        f"cd {PROJECT_DIR} && "
        f"{PYTHON_BIN} scripts/collect_ranking_scrapling.py "
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

def _send_slack_notification(**context):
    """DAG 실행 결과를 Slack으로 발송 (토큰 없으면 스킵)"""
    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKEN이 설정되지 않아 알림을 스킵합니다.")
        return

    ti = context["ti"]
    loaded_rows = ti.xcom_pull(task_ids="load_to_bigquery", key="loaded_rows") or 0
    source_dir = ti.xcom_pull(task_ids="load_to_bigquery", key="source_dir") or ""
    run_id = os.path.basename(source_dir) if source_dir else "unknown"
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🛒 올리브영 랭킹 수집 완료",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*실행 시각*\n{execution_date}"},
                {"type": "mrkdwn", "text": f"*Run ID*\n`{run_id}`"},
                {"type": "mrkdwn", "text": f"*적재 건수*\n{loaded_rows:,}건"},
                {"type": "mrkdwn", "text": f"*카테고리*\n{CATEGORY_LIMIT}개"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*BigQuery 테이블*\n`member-378109.jaeho.oliveyoung_ranking`",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "📊 Airflow DAG: `oliveyoung_ranking_collector` | 매시 30분 자동 수집",
                }
            ],
        },
    ]

    payload = json.dumps({
        "channel": SLACK_CHANNEL_ID,
        "blocks": blocks,
        "text": f"올리브영 랭킹 수집 완료: {loaded_rows:,}건 적재",
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        },
    )

    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Slack 발송 실패: {result.get('error')}")
    print(f"Slack 알림 발송 완료: channel={SLACK_CHANNEL_ID}")


# Task 4: Slack 알림
notify_slack = PythonOperator(
    task_id="notify_slack",
    python_callable=_send_slack_notification,
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)

# 파이프라인: 크롤링 → BigQuery 적재 → 정리 → Slack 알림
crawl_ranking >> load_to_bq >> cleanup >> notify_slack
