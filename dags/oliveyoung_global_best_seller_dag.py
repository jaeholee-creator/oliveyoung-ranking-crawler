"""
글로벌 올리브영 베스트셀러 수집 DAG

- 스케줄: 매시 55분 (55 * * * *)
- 파이프라인: 크롤링 → BigQuery 적재 → 정리 → Slack 알림
- 대상: global.oliveyoung.com (Top Orders + Top in Korea)
- 저장: member-378109.jaeho.global_oliveyoung_best_seller
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

PROJECT_DIR = Variable.get("oliveyoung_project_dir", default_var="/opt/airflow/app")
GCP_KEY_PATH = Variable.get("gcp_key_path", default_var="/opt/airflow/keys/target-378109-7f1aa3e0dc9a.json")
SLACK_BOT_TOKEN = Variable.get("slack_bot_token", default_var="")
SLACK_CHANNEL_ID = Variable.get("slack_channel_id", default_var="C0ACH02BLG5")
PYTHON_BIN = Variable.get("oliveyoung_python_bin", default_var="/home/ubuntu/airflow-venv/bin/python3")

# 글로벌 베스트셀러 수집 설정
GLOBAL_TABS = Variable.get("global_best_seller_tabs", default_var="1,2")          # 1=Top Orders, 2=Top in Korea
GLOBAL_MAX_RANK = int(Variable.get("global_best_seller_max_rank", default_var="100"))
GLOBAL_ENRICH_REVIEWS = Variable.get("global_best_seller_enrich_reviews", default_var="true").lower() == "true"

TARGET_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
BQ_TABLE = "member-378109.jaeho.global_oliveyoung_best_seller"

default_args = {
    "owner": "jaeho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="oliveyoung_global_best_seller",
    default_args=default_args,
    description="글로벌 올리브영 베스트셀러 수집 및 BigQuery 적재 (매시 55분)",
    schedule_interval="30 * * * *",  # 매시 30분
    start_date=datetime(2026, 2, 27),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "global", "best_seller", "bigquery"],
)


# Task 1: 글로벌 베스트셀러 크롤링
enrich_flag = "--enrich-missing-review-metrics" if GLOBAL_ENRICH_REVIEWS else ""

crawl_global = BashOperator(
    task_id="crawl_global_best_seller",
    bash_command=(
        f"export PYTHONUNBUFFERED=1 && "
        f"cd {PROJECT_DIR} && "
        f"{PYTHON_BIN} scripts/collect_global_best_seller.py "
        f'--target-url "{TARGET_URL}" '
        f"--tabs {GLOBAL_TABS} "
        f"--max-rank {GLOBAL_MAX_RANK} "
        f"--out-dir output/global_best_seller "
        f"--enrich-workers 4 {enrich_flag}"
    ),
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)


def _load_latest_to_bigquery(**context):
    """가장 최근 수집 데이터를 BigQuery에 적재"""
    import sys
    sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH

    output_base = os.path.join(PROJECT_DIR, "output", "global_best_seller")
    run_dirs = sorted(glob.glob(os.path.join(output_base, "*Z")))
    if not run_dirs:
        raise FileNotFoundError(f"수집 데이터 없음: {output_base}")

    latest_dir = run_dirs[-1]
    json_path = os.path.join(latest_dir, "global_best_seller_rows.json")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON 파일 없음: {json_path}")

    with open(json_path) as f:
        rows = json.load(f)

    if not rows:
        raise ValueError(f"수집 데이터 0건 — BQ 적재 스킵: {json_path}")

    from load_global_to_bigquery import load_to_bigquery
    row_count = load_to_bigquery(json_path)

    context["ti"].xcom_push(key="loaded_rows", value=row_count)
    context["ti"].xcom_push(key="source_dir", value=latest_dir)

    # summary에서 카테고리 수 추출
    summary_path = os.path.join(latest_dir, "summary.json")
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)
        category_count = len(summary.get("category_summaries") or [])
        error_count = len(summary.get("errors") or [])
        context["ti"].xcom_push(key="category_count", value=category_count)
        context["ti"].xcom_push(key="error_count", value=error_count)

    return row_count


load_to_bq = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=_load_latest_to_bigquery,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)


def _cleanup_old_runs(**_context):
    """7일 이상 된 로컬 수집 데이터 정리"""
    import shutil
    from datetime import timezone
    output_base = os.path.join(PROJECT_DIR, "output", "global_best_seller")
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)

    removed = 0
    for run_dir in glob.glob(os.path.join(output_base, "*Z")):
        dir_name = os.path.basename(run_dir)
        try:
            ts = datetime.strptime(dir_name, "%Y%m%dT%H%M%SZ")
            if ts < cutoff:
                shutil.rmtree(run_dir)
                removed += 1
        except ValueError:
            continue

    print(f"정리 완료: {removed}개 디렉토리 삭제")
    return removed


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
    category_count = ti.xcom_pull(task_ids="load_to_bigquery", key="category_count") or 0
    error_count = ti.xcom_pull(task_ids="load_to_bigquery", key="error_count") or 0
    run_id = os.path.basename(source_dir) if source_dir else "unknown"
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")

    status_emoji = "⚠️" if error_count > 0 else "✅"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🌍 글로벌 올리브영 베스트셀러 수집 완료",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*실행 시각*\n{execution_date}"},
                {"type": "mrkdwn", "text": f"*Run ID*\n`{run_id}`"},
                {"type": "mrkdwn", "text": f"*적재 건수*\n{loaded_rows:,}건"},
                {"type": "mrkdwn", "text": f"*카테고리*\n{category_count}개 {status_emoji}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*BigQuery 테이블*\n`{BQ_TABLE}`",
            },
        },
    ]

    if error_count > 0:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"⚠️ *카테고리 수집 오류*: {error_count}건 실패",
            },
        })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "📊 Airflow DAG: `oliveyoung_global_best_seller` | 매시 55분 자동 수집",
            }
        ],
    })

    payload = json.dumps({
        "channel": SLACK_CHANNEL_ID,
        "blocks": blocks,
        "text": f"글로벌 올리브영 베스트셀러 수집 완료: {loaded_rows:,}건 적재",
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


notify_slack = PythonOperator(
    task_id="notify_slack",
    python_callable=_send_slack_notification,
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)

# 파이프라인: 크롤링 → BigQuery 적재 → 정리 → Slack 알림
crawl_global >> load_to_bq >> cleanup >> notify_slack
