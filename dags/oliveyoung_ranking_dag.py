"""
올리브영 판매 랭킹 수집 DAG

- 스케줄: 매시 1회
- 파이프라인: 크롤링(camoufox) → BigQuery 적재(Python) → 정리 → Slack 알림
- 0건 수집 시 실패 처리 (crawl_ranking exit 1 → Airflow retry)
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
CATEGORY_LIMIT = int(Variable.get("oliveyoung_category_limit", default_var="21"))
CATEGORY_WAIT_MS = int(Variable.get("oliveyoung_category_wait_ms", default_var="2200"))
CATEGORY_ROWS = int(Variable.get("oliveyoung_category_rows", default_var="100"))
REVIEW_WORKERS = int(Variable.get("oliveyoung_review_workers", default_var="18"))
REVIEW_RETRIES = int(Variable.get("oliveyoung_review_retries", default_var="3"))
REVIEW_TIMEOUT = float(Variable.get("oliveyoung_review_timeout", default_var="10"))
DETAIL_WORKERS = int(Variable.get("oliveyoung_detail_workers", default_var="6"))
DETAIL_RETRIES = int(Variable.get("oliveyoung_detail_retries", default_var="2"))
DETAIL_TIMEOUT = float(Variable.get("oliveyoung_detail_timeout", default_var="15"))
CATEGORY_RETRIES = int(Variable.get("oliveyoung_category_retries", default_var="2"))
PAGE_TIMEOUT_MS = int(Variable.get("oliveyoung_page_timeout_ms", default_var="45000"))
CATEGORY_SCROLL_ATTEMPTS = int(Variable.get("oliveyoung_category_scroll_attempts", default_var="3"))
CRAWL_TIMEOUT_MINUTES = int(Variable.get("oliveyoung_crawl_timeout_minutes", default_var="20"))
REVIEW_CACHE_PATH = Variable.get("oliveyoung_review_cache_path", default_var="cache/review_stats_cache.json")
REVIEW_CACHE_TTL_HOURS = float(Variable.get("oliveyoung_review_cache_ttl_hours", default_var="8"))
REVIEW_CACHE_MAX_ENTRIES = int(Variable.get("oliveyoung_review_cache_max_entries", default_var="30000"))
CLEANUP_RETENTION_DAYS = int(Variable.get("oliveyoung_cleanup_days", default_var="7"))
CRAWL_RETRIES = int(Variable.get("oliveyoung_airflow_retries", default_var="2"))
CRAWL_RETRY_DELAY_MINUTES = int(Variable.get("oliveyoung_airflow_retry_delay", default_var="3"))
MIN_REVIEW_RATE = float(Variable.get("oliveyoung_min_review_success_rate", default_var="80.0"))
SLACK_BOT_TOKEN = Variable.get("slack_bot_token", default_var="")
SLACK_CHANNEL_ID = Variable.get("slack_channel_id", default_var="C0ACH02BLG5")
BRAND_REPORT_TARGETS = Variable.get("brand_report_targets", default_var="바이오던스").split(",")
PYTHON_BIN = Variable.get("oliveyoung_python_bin", default_var="/home/ubuntu/airflow-venv/bin/python3")

TARGET_URL = (
    "https://www.oliveyoung.co.kr/store/main/getBestList.do?"
    "t_page=2%EC%9B%94%20%EC%98%AC%EC%98%81PICK%20%EA%B8%B0%ED%9A%8D%EC%A0%84"
    "%20%EC%83%81%EC%84%B8&t_click=GNB&t_gnb_type=%EB%9E%AD%ED%82%B9&t_swiping_type=N"
)

# 최소 수집 건수 (미달 시 크롤러가 exit 1 → Airflow가 retry)
MIN_ROWS = 2100


def _send_slack_alert(channel, token, blocks, fallback_text):
    """Slack 알림 발송 헬퍼"""
    payload = json.dumps({
        "channel": channel,
        "blocks": blocks,
        "text": fallback_text,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Slack 발송 실패: {result.get('error')}")


def _on_failure_callback(context):
    """태스크 실패 시 Slack 알림"""
    if not SLACK_BOT_TOKEN:
        return

    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")
    exception = str(context.get("exception", "Unknown error"))

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🚨 올리브영 랭킹 수집 실패"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task*\n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*실행 시각*\n{execution_date}"},
                {"type": "mrkdwn", "text": f"*시도 횟수*\n{ti.try_number}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*에러*\n```{exception[:500]}```"},
        },
    ]

    try:
        _send_slack_alert(SLACK_CHANNEL_ID, SLACK_BOT_TOKEN, blocks,
                          f"🚨 올리브영 랭킹 수집 실패: {task_id}")
    except Exception as exc:
        print(f"[WARN] 실패 알림 발송 오류: {exc}")


default_args = {
    "owner": "jaeho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": CRAWL_RETRIES,
    "retry_delay": timedelta(minutes=CRAWL_RETRY_DELAY_MINUTES),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=8),
    "on_failure_callback": _on_failure_callback,
}

dag = DAG(
    dag_id="oliveyoung_ranking_collector",
    default_args=default_args,
    description="올리브영 판매 랭킹 수집 및 BigQuery 적재 (매시 1회)",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 2, 25),
    catchup=False,
    max_active_runs=1,
    tags=["oliveyoung", "ranking", "crawler", "bigquery"],
)


# Task 1: camoufox로 랭킹 크롤링 + 리뷰 통계 수집
crawl_ranking = BashOperator(
    task_id="crawl_ranking",
    bash_command=(
        f"export DISPLAY=:99 && "
        f"export PYTHONUNBUFFERED=1 && "
        f"cd {PROJECT_DIR} && "
        f"{PYTHON_BIN} scripts/collect_ranking_scrapling.py "
        f'--url "{TARGET_URL}" '
        f"--category-limit {CATEGORY_LIMIT} "
        f"--category-wait-ms {CATEGORY_WAIT_MS} "
        f"--category-rows {CATEGORY_ROWS} "
        f"--min-rows {MIN_ROWS} "
        f"--review-workers {REVIEW_WORKERS} "
        f"--review-retries {REVIEW_RETRIES} "
        f"--review-timeout {REVIEW_TIMEOUT} "
        f"--detail-workers {DETAIL_WORKERS} "
        f"--detail-retries {DETAIL_RETRIES} "
        f"--detail-timeout {DETAIL_TIMEOUT} "
        f"--category-retries {CATEGORY_RETRIES} "
        f"--page-timeout-ms {PAGE_TIMEOUT_MS} "
        f"--category-scroll-attempts {CATEGORY_SCROLL_ATTEMPTS} "
        f"--review-cache-path {REVIEW_CACHE_PATH} "
        f"--review-cache-ttl-hours {REVIEW_CACHE_TTL_HOURS} "
        f"--review-cache-max-entries {REVIEW_CACHE_MAX_ENTRIES} "
        f"--out-dir output/ranking_playwright"
    ),
    execution_timeout=timedelta(minutes=CRAWL_TIMEOUT_MINUTES),
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

    # JSON 파일 로드하여 건수 확인
    with open(json_path) as f:
        rows = json.load(f)

    if not rows:
        raise ValueError(f"수집 데이터 0건 — BQ 적재 스킵: {json_path}")

    summary_path = os.path.join(latest_dir, "summary.json")
    summary: dict[str, object] = {}
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)

    summary_rows = summary.get("rows")
    try:
        if summary_rows is not None and int(summary_rows) < MIN_ROWS:
            raise ValueError(f"수집 요약건수({summary_rows}) 미달 — 재수집 대상: {latest_dir}")
    except (TypeError, ValueError):
        pass

    categories_requested = summary.get("categoriesRequested")
    categories_captured = summary.get("categoriesCaptured")
    if categories_requested and categories_captured is not None:
        try:
            if int(categories_captured) < int(categories_requested):
                print(f"[WARN] 카테고리 수집 부족: {categories_captured}/{categories_requested}")
        except (TypeError, ValueError):
            pass

    from load_to_bigquery import load_to_bigquery
    row_count = load_to_bigquery(json_path)

    context["ti"].xcom_push(key="loaded_rows", value=row_count)
    context["ti"].xcom_push(key="source_dir", value=latest_dir)
    if categories_captured is not None:
        try:
            context["ti"].xcom_push(key="categories_captured", value=int(categories_captured))
        except (TypeError, ValueError):
            context["ti"].xcom_push(key="categories_captured", value=categories_captured)

    # 리뷰 통계 xcom push
    rv = summary.get("reviewStats") or {}
    context["ti"].xcom_push(key="review_total", value=rv.get("total"))
    context["ti"].xcom_push(key="review_success", value=rv.get("success"))
    context["ti"].xcom_push(key="review_fail", value=rv.get("fail"))
    context["ti"].xcom_push(key="review_rate", value=rv.get("rate"))

    review_rate = 0.0
    try:
        review_rate = float(rv.get("rate") or 0.0)
    except (TypeError, ValueError):
        review_rate = 0.0
    context["ti"].xcom_push(key="review_alert", value=review_rate < MIN_REVIEW_RATE)

    return row_count


load_to_bq = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=_load_latest_to_bigquery,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)


def _cleanup_old_runs(**context):
    """보관 기간 경과된 로컬 수집 데이터 정리"""
    output_base = os.path.join(PROJECT_DIR, "output", "ranking_playwright")
    cutoff = datetime.utcnow() - timedelta(days=CLEANUP_RETENTION_DAYS)

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


cleanup = PythonOperator(
    task_id="cleanup_old_runs",
    python_callable=_cleanup_old_runs,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

def _send_slack_notification(**context):
    """문제 발생 시에만 Slack 알림 (정상 수행 시 스킵)"""
    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKEN이 설정되지 않아 알림을 스킵합니다.")
        return

    ti = context["ti"]
    loaded_rows = ti.xcom_pull(task_ids="load_to_bigquery", key="loaded_rows") or 0
    source_dir = ti.xcom_pull(task_ids="load_to_bigquery", key="source_dir") or ""
    run_id = os.path.basename(source_dir) if source_dir else "unknown"
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")
    categories_captured = ti.xcom_pull(task_ids="load_to_bigquery", key="categories_captured") or CATEGORY_LIMIT

    review_total = ti.xcom_pull(task_ids="load_to_bigquery", key="review_total") or 0
    review_success = ti.xcom_pull(task_ids="load_to_bigquery", key="review_success") or 0
    review_fail = ti.xcom_pull(task_ids="load_to_bigquery", key="review_fail") or 0
    review_rate = ti.xcom_pull(task_ids="load_to_bigquery", key="review_rate") or 0.0
    review_alert = ti.xcom_pull(task_ids="load_to_bigquery", key="review_alert") or False

    # 문제 감지
    issues = []
    if review_alert:
        issues.append(f"리뷰 성공률 저조: {review_rate}% (임계값 {MIN_REVIEW_RATE}%)")
    try:
        if int(categories_captured) < CATEGORY_LIMIT:
            issues.append(f"카테고리 수집 부족: {categories_captured}/{CATEGORY_LIMIT}개")
    except (TypeError, ValueError):
        pass
    if loaded_rows < MIN_ROWS:
        issues.append(f"수집 건수 부족: {loaded_rows:,}건 (최소 {MIN_ROWS:,}건)")

    if not issues:
        print(f"[OK] 정상 수집 완료 — {loaded_rows:,}건 적재, 리뷰 {review_rate}%, 알림 스킵")
        return

    if review_fail == 0:
        review_status = f"✅ {review_success:,}/{review_total:,} ({review_rate}%)"
    elif review_rate >= 99.0:
        review_status = f"🟡 {review_success:,}/{review_total:,} ({review_rate}%) — {review_fail}건 누락"
    else:
        review_status = f"🔴 {review_success:,}/{review_total:,} ({review_rate}%) — {review_fail}건 실패"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "⚠️ 올리브영 랭킹 수집 — 문제 감지",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*실행 시각*\n{execution_date}"},
                {"type": "mrkdwn", "text": f"*Run ID*\n`{run_id}`"},
                {"type": "mrkdwn", "text": f"*적재 건수*\n{loaded_rows:,}건"},
                {"type": "mrkdwn", "text": f"*카테고리*\n{categories_captured}/{CATEGORY_LIMIT}개"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*리뷰 통계 수집*\n{review_status}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*감지된 문제*\n" + "\n".join(f"• {issue}" for issue in issues),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "📊 Airflow DAG: `oliveyoung_ranking_collector` | 매시 1회 자동 수집",
                }
            ],
        },
    ]

    _send_slack_alert(SLACK_CHANNEL_ID, SLACK_BOT_TOKEN, blocks,
                      f"⚠️ 올리브영 랭킹 수집 문제: {', '.join(issues)}")
    print(f"Slack 알림 발송 완료: {issues}")


notify_slack = PythonOperator(
    task_id="notify_slack",
    python_callable=_send_slack_notification,
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)


def _send_brand_ranking_report(**context):
    """브랜드별 시간별 랭킹 변동을 BigQuery에서 조회해 Slack으로 발송"""
    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKEN이 설정되지 않아 브랜드 리포트를 스킵합니다.")
        return

    import sys
    sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH
    os.environ["SLACK_BOT_TOKEN"] = SLACK_BOT_TOKEN
    os.environ["SLACK_CHANNEL_ID"] = SLACK_CHANNEL_ID

    from brand_ranking_report import get_bq_client, fetch_ranking_comparison, build_slack_blocks, send_slack

    client = get_bq_client()

    for brand in BRAND_REPORT_TARGETS:
        brand = brand.strip()
        if not brand:
            continue
        print(f"[{brand}] 랭킹 변동 리포트 조회 중...")
        try:
            data = fetch_ranking_comparison(client, brand)
            if not data:
                print(f"[{brand}] 데이터 없음, 스킵")
                continue
            blocks = build_slack_blocks(brand, data)
            send_slack(blocks, brand, SLACK_CHANNEL_ID, SLACK_BOT_TOKEN)
            print(f"[{brand}] 발송 완료")
        except Exception as exc:
            print(f"[WARN] {brand} 리포트 처리 실패: {exc}")


brand_ranking_report = PythonOperator(
    task_id="brand_ranking_report",
    python_callable=_send_brand_ranking_report,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

# 파이프라인: 크롤링 → BigQuery 적재 → 정리 → 수집완료 알림 → 브랜드 랭킹 변동 리포트
crawl_ranking >> load_to_bq >> cleanup >> notify_slack >> brand_ranking_report
