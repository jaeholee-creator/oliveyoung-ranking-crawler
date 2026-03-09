#!/usr/bin/env python3
"""올리브영 상품 식별 정보 보강 스크립트.

최근 랭킹에 등장한 goods_no 중 아직 식별 정보가 없는 상품만 골라
상세 페이지를 재수집하고, 성공 시 product_identity 테이블에 upsert,
실패 시 retry 큐에 upsert한다.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collect_ranking_scrapling import (  # noqa: E402
    CF_WAIT_SECONDS,
    DETAIL_TIMEOUT,
    DETAIL_WORKERS,
    DETAIL_PROXY_POOL_ENV,
    REVIEW_TIMEOUT,
    USE_CAMOUFOX,
    Camoufox,
    detect_challenge,
    fetch_detail_enrichment_http,
    parse_proxy_pool,
    simulate_human,
    wait_for_cf_resolution,
)

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
RANKING_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_ranking_history"
IDENTITY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_product_details"
RETRY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_product_detail_retry_queue"
DEFAULT_TARGET_URL = "https://www.oliveyoung.co.kr/store/main/getBestList.do"
DEFAULT_BOOTSTRAP_URL = (
    "https://www.oliveyoung.co.kr/store/main/getBestList.do?"
    "t_page=2%EC%9B%94%20%EC%98%AC%EC%98%81PICK%20%EA%B8%B0%ED%9A%8D%EC%A0%84"
    "%20%EC%83%81%EC%84%B8&t_click=GNB&t_gnb_type=%EB%9E%AD%ED%82%B9&t_swiping_type=N"
)
KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="올리브영 상품 식별 정보 보강")
    parser.add_argument("--lookback-hours", type=int, default=24 * 14, help="신규 goods_no 후보 탐색 기간")
    parser.add_argument("--limit", type=int, default=300, help="한 번에 처리할 최대 goods_no 수")
    parser.add_argument("--detail-workers", type=int, default=DETAIL_WORKERS, help="상세 보강 동시 요청 수")
    parser.add_argument("--detail-timeout", type=float, default=DETAIL_TIMEOUT, help="상세 보강 타임아웃(초)")
    parser.add_argument("--detail-retries", type=int, default=2, help="상세 보강 재시도 횟수")
    parser.add_argument(
        "--detail-proxy-pool",
        default=os.environ.get(DETAIL_PROXY_POOL_ENV, ""),
        help="상세 보강 HTTP 요청에만 적용할 프록시 풀(쉼표 구분, worker-affinity)",
    )
    parser.add_argument("--seed-limit", type=int, default=5000, help="랭킹 테이블에서 바로 승격할 최대 goods_no 수")
    parser.add_argument("--retry-base-minutes", type=int, default=30, help="실패 재시도 기본 간격(분)")
    parser.add_argument("--max-retry-count", type=int, default=12, help="재시도 최대 횟수")
    parser.add_argument(
        "--skip-retry-queue",
        action="store_true",
        default=False,
        help="재시도 큐를 건너뛰고 최근 랭킹의 신규 goods_no만 상세 보강",
    )
    parser.add_argument("--page-timeout-ms", type=int, default=45000, help="쿠키 부트스트랩 페이지 타임아웃")
    parser.add_argument("--bootstrap-url", default=DEFAULT_BOOTSTRAP_URL, help="CF 쿠키 부트스트랩용 URL")
    parser.add_argument("--browser-profile-dir", default=".browser_profile_camoufox", help="Camoufox 프로필 디렉토리")
    parser.add_argument(
        "--disable-rescue-pass",
        action="store_true",
        default=False,
        help="실패 goods_no에 대한 추가 rescue pass를 수행하지 않음",
    )
    parser.add_argument(
        "--rescue-workers",
        type=int,
        default=1,
        help="실패 goods_no rescue pass 동시 요청 수",
    )
    parser.add_argument(
        "--rescue-retries",
        type=int,
        default=1,
        help="실패 goods_no rescue pass 재시도 횟수",
    )
    parser.add_argument("--dry-run", action="store_true", default=False, help="BQ 쓰기 없이 후보/성공 건수만 출력")
    return parser.parse_args()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def get_bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def normalize_product_name(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = re.sub(r"\[[^\]]*\]", " ", name)
    cleaned = re.sub(r"\([^)]*특가[^)]*\)", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    return cleaned or None


def bootstrap_cookies(target_url: str, page_timeout_ms: int, profile_dir_name: str) -> dict[str, str]:
    if not USE_CAMOUFOX:
        return {}

    profile_dir = Path.cwd() / profile_dir_name
    profile_dir.mkdir(parents=True, exist_ok=True)
    try:
        with Camoufox(
            persistent_context=True,
            headless=True,
            user_data_dir=str(profile_dir),
            locale="ko-KR",
            humanize=True,
            enable_cache=True,
        ) as context:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=page_timeout_ms)
            page.wait_for_timeout(2500)
            if detect_challenge(page):
                if not wait_for_cf_resolution(page, CF_WAIT_SECONDS):
                    print("[WARN] 쿠키 부트스트랩 중 CF 미해결")
                    return {}
            simulate_human(page)
            return {
                cookie["name"]: cookie["value"]
                for cookie in context.cookies()
                if cookie.get("name") and cookie.get("value")
            }
    except Exception as exc:
        print(f"[WARN] 쿠키 부트스트랩 실패: {exc}")
        return {}


def fetch_candidates(
    client: bigquery.Client,
    lookback_hours: int,
    limit: int,
    max_retry_count: int,
    *,
    skip_retry_queue: bool = False,
) -> list[dict[str, Any]]:
    if skip_retry_queue:
        query = f"""
        WITH recent_ranking AS (
          SELECT
            goods_no,
            ARRAY_AGG(
              STRUCT(detail_url, brand_name, product_name, collected_at)
              ORDER BY collected_at DESC LIMIT 1
            )[OFFSET(0)] AS latest,
            MIN(collected_at) AS first_seen_at,
            MAX(collected_at) AS last_seen_at
          FROM `{RANKING_TABLE_ID}`
          WHERE goods_no IS NOT NULL
            AND goods_no != ''
            AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
          GROUP BY goods_no
        )
        SELECT
          r.goods_no,
          r.latest.detail_url AS detail_url,
          r.latest.brand_name AS brand_name,
          r.latest.product_name AS product_name,
          r.first_seen_at,
          r.first_seen_at AS first_queued_at,
          r.last_seen_at,
          0 AS retry_count,
          'NEW' AS candidate_type
        FROM recent_ranking r
        LEFT JOIN `{IDENTITY_TABLE_ID}` i USING (goods_no)
        WHERE i.goods_no IS NULL
        ORDER BY r.last_seen_at DESC
        LIMIT @limit
        """
    else:
        query = f"""
    WITH recent_ranking AS (
      SELECT
        goods_no,
        ARRAY_AGG(
          STRUCT(detail_url, brand_name, product_name, collected_at)
          ORDER BY collected_at DESC LIMIT 1
        )[OFFSET(0)] AS latest,
        MIN(collected_at) AS first_seen_at,
        MAX(collected_at) AS last_seen_at
      FROM `{RANKING_TABLE_ID}`
      WHERE goods_no IS NOT NULL
        AND goods_no != ''
        AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
      GROUP BY goods_no
    ),
    retry_ready AS (
      SELECT
        goods_no,
        source_detail_url,
        first_queued_at,
        retry_count,
        last_seen_at,
        next_retry_at
      FROM `{RETRY_TABLE_ID}`
      WHERE last_status = 'PENDING'
        AND retry_count < @max_retry_count
        AND next_retry_at <= CURRENT_TIMESTAMP()
    ),
    retry_candidates AS (
      SELECT
        q.goods_no,
        COALESCE(q.source_detail_url, r.latest.detail_url) AS detail_url,
        COALESCE(r.latest.brand_name, '') AS brand_name,
        COALESCE(r.latest.product_name, '') AS product_name,
        COALESCE(r.first_seen_at, q.last_seen_at) AS first_seen_at,
        q.first_queued_at AS first_queued_at,
        COALESCE(r.last_seen_at, q.last_seen_at) AS last_seen_at,
        q.retry_count AS retry_count,
        'RETRY' AS candidate_type
      FROM retry_ready q
      LEFT JOIN recent_ranking r USING (goods_no)
      LEFT JOIN `{IDENTITY_TABLE_ID}` i USING (goods_no)
      WHERE i.goods_no IS NULL
    ),
    new_candidates AS (
      SELECT
        r.goods_no,
        r.latest.detail_url AS detail_url,
        r.latest.brand_name AS brand_name,
        r.latest.product_name AS product_name,
        r.first_seen_at,
        r.first_seen_at AS first_queued_at,
        r.last_seen_at,
        0 AS retry_count,
        'NEW' AS candidate_type
      FROM recent_ranking r
      LEFT JOIN `{IDENTITY_TABLE_ID}` i USING (goods_no)
      LEFT JOIN retry_ready q USING (goods_no)
      WHERE i.goods_no IS NULL
        AND q.goods_no IS NULL
    )
    SELECT *
    FROM (
      SELECT * FROM retry_candidates
      UNION ALL
      SELECT * FROM new_candidates
    )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY goods_no
      ORDER BY
        CASE candidate_type WHEN 'RETRY' THEN 0 ELSE 1 END,
        last_seen_at DESC
    ) = 1
    ORDER BY
      CASE candidate_type WHEN 'RETRY' THEN 0 ELSE 1 END,
      last_seen_at DESC
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("max_retry_count", "INT64", max_retry_count),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def fetch_seed_rows(
    client: bigquery.Client,
    lookback_hours: int,
    limit: int,
) -> list[dict[str, Any]]:
    query = f"""
    WITH ranked AS (
      SELECT
        goods_no,
        detail_url,
        brand_name,
        product_name,
        item_id,
        standard_code,
        option_number,
        option_name,
        option_image_url,
        goods_type_code,
        goods_section_code,
        trade_code,
        delivery_policy_number,
        online_brand_code,
        online_brand_name,
        online_brand_eng_name,
        brand_code,
        supplier_code,
        supplier_name,
        status_code,
        status_name,
        sold_out_flag,
        registered_at,
        modified_at,
        display_start_at,
        display_end_at,
        standard_category_upper_code,
        standard_category_upper_name,
        standard_category_middle_code,
        standard_category_middle_name,
        standard_category_lower_code,
        standard_category_lower_name,
        display_category_upper_number,
        display_category_upper_name,
        display_category_middle_number,
        display_category_middle_name,
        display_category_lower_number,
        display_category_lower_name,
        display_category_leaf_number,
        display_category_leaf_name,
        og_url,
        og_image_url,
        eg_item_url,
        qna_count,
        description_type_code,
        description_image_count,
        description_image_urls_json,
        detail_meta_json,
        extra_data_json,
        collected_at,
        MIN(collected_at) OVER (PARTITION BY goods_no) AS first_seen_at,
        MAX(collected_at) OVER (PARTITION BY goods_no) AS last_seen_at,
        ROW_NUMBER() OVER (PARTITION BY goods_no ORDER BY collected_at DESC) AS row_num
      FROM `{RANKING_TABLE_ID}`
      WHERE goods_no IS NOT NULL
        AND goods_no != ''
        AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
        AND (
          item_id IS NOT NULL OR standard_code IS NOT NULL OR detail_meta_json IS NOT NULL
          OR description_image_urls_json IS NOT NULL
        )
    )
    SELECT r.*
    FROM ranked r
    LEFT JOIN `{IDENTITY_TABLE_ID}` i USING (goods_no)
    WHERE r.row_num = 1
      AND i.goods_no IS NULL
    ORDER BY r.last_seen_at DESC
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def identity_confidence(payload: dict[str, Any]) -> str:
    item_id = payload.get("item_id")
    standard_code = payload.get("standard_code")
    if item_id and standard_code and item_id == standard_code:
        return "HIGH"
    if item_id or standard_code:
        return "MEDIUM"
    if payload.get("detail_meta_json") or payload.get("description_image_urls_json"):
        return "LOW"
    return "NONE"


def is_success(payload: dict[str, Any]) -> bool:
    return identity_confidence(payload) != "NONE"


def build_identity_rows(
    candidates: list[dict[str, Any]],
    enrichment_map: dict[str, dict[str, Any]],
    enriched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        goods_no = candidate["goods_no"]
        payload = enrichment_map.get(goods_no) or {}
        if not is_success(payload):
            continue

        item_id = payload.get("item_id")
        standard_code = payload.get("standard_code")
        canonical_product_id = item_id or standard_code or goods_no
        row = {
            "goods_no": goods_no,
            "canonical_product_id": canonical_product_id,
            "item_id": item_id,
            "standard_code": standard_code,
            "option_number": payload.get("option_number"),
            "option_name": payload.get("option_name"),
            "option_image_url": payload.get("option_image_url"),
            "source_detail_url": candidate.get("detail_url"),
            "brand_name": candidate.get("brand_name"),
            "product_name": candidate.get("product_name"),
            "normalized_product_name": normalize_product_name(candidate.get("product_name")),
            "brand_code": payload.get("brand_code"),
            "supplier_code": payload.get("supplier_code"),
            "supplier_name": payload.get("supplier_name"),
            "online_brand_code": payload.get("online_brand_code"),
            "online_brand_name": payload.get("online_brand_name"),
            "online_brand_eng_name": payload.get("online_brand_eng_name"),
            "goods_type_code": payload.get("goods_type_code"),
            "goods_section_code": payload.get("goods_section_code"),
            "trade_code": payload.get("trade_code"),
            "delivery_policy_number": payload.get("delivery_policy_number"),
            "status_code": payload.get("status_code"),
            "status_name": payload.get("status_name"),
            "sold_out_flag": payload.get("sold_out_flag"),
            "registered_at": payload.get("registered_at"),
            "modified_at": payload.get("modified_at"),
            "display_start_at": payload.get("display_start_at"),
            "display_end_at": payload.get("display_end_at"),
            "standard_category_upper_code": payload.get("standard_category_upper_code"),
            "standard_category_upper_name": payload.get("standard_category_upper_name"),
            "standard_category_middle_code": payload.get("standard_category_middle_code"),
            "standard_category_middle_name": payload.get("standard_category_middle_name"),
            "standard_category_lower_code": payload.get("standard_category_lower_code"),
            "standard_category_lower_name": payload.get("standard_category_lower_name"),
            "display_category_upper_number": payload.get("display_category_upper_number"),
            "display_category_upper_name": payload.get("display_category_upper_name"),
            "display_category_middle_number": payload.get("display_category_middle_number"),
            "display_category_middle_name": payload.get("display_category_middle_name"),
            "display_category_lower_number": payload.get("display_category_lower_number"),
            "display_category_lower_name": payload.get("display_category_lower_name"),
            "display_category_leaf_number": payload.get("display_category_leaf_number"),
            "display_category_leaf_name": payload.get("display_category_leaf_name"),
            "og_url": payload.get("og_url"),
            "og_image_url": payload.get("og_image_url"),
            "eg_item_url": payload.get("eg_item_url"),
            "qna_count": payload.get("qna_count"),
            "description_type_code": payload.get("description_type_code"),
            "description_image_count": payload.get("description_image_count"),
            "description_image_urls_json": payload.get("description_image_urls_json"),
            "detail_meta_json": payload.get("detail_meta_json"),
            "extra_data_json": payload.get("extra_data_json"),
            "identity_confidence": identity_confidence(payload),
            "first_seen_at": candidate.get("first_seen_at"),
            "last_seen_at": candidate.get("last_seen_at"),
            "first_enriched_at": enriched_at,
            "last_enriched_at": enriched_at,
            "last_status": "SUCCESS",
        }
        rows.append(row)
    return rows


def build_seed_identity_rows(seed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed in seed_rows:
        canonical_product_id = seed.get("item_id") or seed.get("standard_code") or seed["goods_no"]
        payload = {
            "item_id": seed.get("item_id"),
            "standard_code": seed.get("standard_code"),
            "detail_meta_json": seed.get("detail_meta_json"),
            "description_image_urls_json": seed.get("description_image_urls_json"),
        }
        rows.append(
            {
                "goods_no": seed["goods_no"],
                "canonical_product_id": canonical_product_id,
                "item_id": seed.get("item_id"),
                "standard_code": seed.get("standard_code"),
                "option_number": seed.get("option_number"),
                "option_name": seed.get("option_name"),
                "option_image_url": seed.get("option_image_url"),
                "source_detail_url": seed.get("detail_url"),
                "brand_name": seed.get("brand_name"),
                "product_name": seed.get("product_name"),
                "normalized_product_name": normalize_product_name(seed.get("product_name")),
                "brand_code": seed.get("brand_code"),
                "supplier_code": seed.get("supplier_code"),
                "supplier_name": seed.get("supplier_name"),
                "online_brand_code": seed.get("online_brand_code"),
                "online_brand_name": seed.get("online_brand_name"),
                "online_brand_eng_name": seed.get("online_brand_eng_name"),
                "goods_type_code": seed.get("goods_type_code"),
                "goods_section_code": seed.get("goods_section_code"),
                "trade_code": seed.get("trade_code"),
                "delivery_policy_number": seed.get("delivery_policy_number"),
                "status_code": seed.get("status_code"),
                "status_name": seed.get("status_name"),
                "sold_out_flag": seed.get("sold_out_flag"),
                "registered_at": seed.get("registered_at"),
                "modified_at": seed.get("modified_at"),
                "display_start_at": seed.get("display_start_at"),
                "display_end_at": seed.get("display_end_at"),
                "standard_category_upper_code": seed.get("standard_category_upper_code"),
                "standard_category_upper_name": seed.get("standard_category_upper_name"),
                "standard_category_middle_code": seed.get("standard_category_middle_code"),
                "standard_category_middle_name": seed.get("standard_category_middle_name"),
                "standard_category_lower_code": seed.get("standard_category_lower_code"),
                "standard_category_lower_name": seed.get("standard_category_lower_name"),
                "display_category_upper_number": seed.get("display_category_upper_number"),
                "display_category_upper_name": seed.get("display_category_upper_name"),
                "display_category_middle_number": seed.get("display_category_middle_number"),
                "display_category_middle_name": seed.get("display_category_middle_name"),
                "display_category_lower_number": seed.get("display_category_lower_number"),
                "display_category_lower_name": seed.get("display_category_lower_name"),
                "display_category_leaf_number": seed.get("display_category_leaf_number"),
                "display_category_leaf_name": seed.get("display_category_leaf_name"),
                "og_url": seed.get("og_url"),
                "og_image_url": seed.get("og_image_url"),
                "eg_item_url": seed.get("eg_item_url"),
                "qna_count": seed.get("qna_count"),
                "description_type_code": seed.get("description_type_code"),
                "description_image_count": seed.get("description_image_count"),
                "description_image_urls_json": seed.get("description_image_urls_json"),
                "detail_meta_json": seed.get("detail_meta_json"),
                "extra_data_json": seed.get("extra_data_json"),
                "identity_confidence": identity_confidence(payload),
                "first_seen_at": seed.get("first_seen_at"),
                "last_seen_at": seed.get("last_seen_at"),
                "first_enriched_at": seed.get("collected_at"),
                "last_enriched_at": seed.get("collected_at"),
                "last_status": "SEEDED_FROM_RANKING",
            }
        )
    return rows


def next_retry_time(retry_base_minutes: int, retry_count: int) -> str:
    multiplier = 2 ** max(0, min(retry_count - 1, 6))
    retry_at = now_utc() + timedelta(minutes=retry_base_minutes * multiplier)
    return retry_at.isoformat()


def build_retry_rows(
    candidates: list[dict[str, Any]],
    enrichment_map: dict[str, dict[str, Any]],
    *,
    retry_base_minutes: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    attempted_at = now_iso()
    for candidate in candidates:
        goods_no = candidate["goods_no"]
        payload = enrichment_map.get(goods_no) or {}
        retry_count = int(candidate.get("retry_count") or 0)
        if is_success(payload):
            rows.append(
                {
                    "goods_no": goods_no,
                    "source_detail_url": candidate.get("detail_url"),
                    "last_seen_at": candidate.get("last_seen_at"),
                    "first_queued_at": candidate.get("first_queued_at") or attempted_at,
                    "last_attempt_at": attempted_at,
                    "next_retry_at": None,
                    "retry_count": retry_count,
                    "last_status": "RESOLVED",
                    "last_error": None,
                    "resolved_at": attempted_at,
                }
            )
            continue

        next_count = retry_count + 1
        rows.append(
            {
                "goods_no": goods_no,
                "source_detail_url": candidate.get("detail_url"),
                "last_seen_at": candidate.get("last_seen_at"),
                "first_queued_at": candidate.get("first_queued_at") or attempted_at,
                "last_attempt_at": attempted_at,
                "next_retry_at": next_retry_time(retry_base_minutes, next_count),
                "retry_count": next_count,
                "last_status": "PENDING",
                "last_error": "NO_IDENTITY_FIELDS",
                "resolved_at": None,
            }
        )
    return rows


def rescue_failed_enrichment(
    candidates: list[dict[str, Any]],
    enrichment_map: dict[str, dict[str, Any]],
    *,
    bootstrap_url: str,
    page_timeout_ms: int,
    browser_profile_dir: str,
    detail_workers: int,
    detail_timeout: float,
    detail_retries: int,
    detail_proxy_pool: list[str],
) -> dict[str, dict[str, Any]]:
    failed_candidates = [
        candidate
        for candidate in candidates
        if not is_success(enrichment_map.get(candidate["goods_no"]) or {})
    ]
    if not failed_candidates:
        return {}

    goods_nos = [candidate["goods_no"] for candidate in failed_candidates]
    detail_url_map = {
        candidate["goods_no"]: candidate.get("detail_url")
        for candidate in failed_candidates
        if candidate.get("detail_url")
    }
    rescue_profile_dir = f"{browser_profile_dir}_rescue"
    rescue_cookies = bootstrap_cookies(bootstrap_url, page_timeout_ms, rescue_profile_dir)
    print(
        f"[INFO] identity rescue 후보: {len(failed_candidates)}건 "
        f"(workers={detail_workers}, retries={detail_retries}, cookies={len(rescue_cookies)})"
    )
    rescue_map = fetch_detail_enrichment_http(
        goods_nos,
        detail_urls=detail_url_map,
        playwright_cookies=rescue_cookies,
        proxy_pool=detail_proxy_pool,
        max_workers=max(1, detail_workers),
        timeout=detail_timeout,
        retries=detail_retries,
    )
    rescued_count = len([payload for payload in rescue_map.values() if is_success(payload)])
    print(f"[INFO] identity rescue 성공: {rescued_count}/{len(failed_candidates)}건")
    return rescue_map


def merge_rows(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
    *,
    merge_key: str = "goods_no",
) -> int:
    if not rows:
        return 0

    def _json_ready(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, list):
            return [_json_ready(item) for item in value]
        if isinstance(value, dict):
            return {key: _json_ready(item) for key, item in value.items()}
        return value

    json_rows = [_json_ready(row) for row in rows]

    target_table = client.get_table(table_id)
    table_name = table_id.split(".")[-1]
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}_staging_{int(time.time())}"

    job_config = bigquery.LoadJobConfig(
        schema=target_table.schema,
        write_disposition="WRITE_TRUNCATE",
    )
    client.load_table_from_json(json_rows, staging_table_id, job_config=job_config).result()

    columns = [field.name for field in target_table.schema]
    update_columns = [column for column in columns if column != merge_key]
    update_clause = ", ".join(f"T.{column} = S.{column}" for column in update_columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"S.{column}" for column in columns)

    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_table_id}` S
    ON T.{merge_key} = S.{merge_key}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    client.query(merge_sql).result()
    client.delete_table(staging_table_id, not_found_ok=True)
    return len(json_rows)


def main() -> None:
    args = parse_args()
    client = get_bq_client()
    detail_proxy_pool = parse_proxy_pool(args.detail_proxy_pool)
    seed_candidates = fetch_seed_rows(client, args.lookback_hours, args.seed_limit)
    seed_rows = build_seed_identity_rows(seed_candidates)
    print(f"[INFO] ranking seed 후보: {len(seed_candidates)}건")
    if args.dry_run:
        print(f"[INFO] ranking seed 즉시 승격 가능: {len(seed_rows)}건")
    elif seed_rows:
        seeded_count = merge_rows(client, IDENTITY_TABLE_ID, seed_rows)
        print(f"[INFO] ranking seed upsert 완료: {seeded_count}건")

    candidates = fetch_candidates(
        client,
        args.lookback_hours,
        args.limit,
        args.max_retry_count,
        skip_retry_queue=args.skip_retry_queue,
    )
    if not candidates:
        print("[INFO] 보강 대상 goods_no 없음")
        return

    goods_nos = [candidate["goods_no"] for candidate in candidates]
    detail_url_map = {
        candidate["goods_no"]: candidate.get("detail_url")
        for candidate in candidates
        if candidate.get("detail_url")
    }

    print(f"[INFO] identity 후보: {len(candidates)}건")
    cookies = bootstrap_cookies(args.bootstrap_url, args.page_timeout_ms, args.browser_profile_dir)
    print(f"[INFO] identity 쿠키 부트스트랩: {len(cookies)}개")

    enrichment_map = fetch_detail_enrichment_http(
        goods_nos,
        detail_urls=detail_url_map,
        playwright_cookies=cookies,
        proxy_pool=detail_proxy_pool,
        max_workers=args.detail_workers,
        timeout=args.detail_timeout,
        retries=args.detail_retries,
    )

    if not args.disable_rescue_pass:
        rescue_map = rescue_failed_enrichment(
            candidates,
            enrichment_map,
            bootstrap_url=args.bootstrap_url,
            page_timeout_ms=args.page_timeout_ms,
            browser_profile_dir=args.browser_profile_dir,
            detail_workers=args.rescue_workers,
            detail_timeout=args.detail_timeout,
            detail_retries=args.rescue_retries,
            detail_proxy_pool=detail_proxy_pool,
        )
        for goods_no, payload in rescue_map.items():
            if is_success(payload):
                enrichment_map[goods_no] = payload

    enriched_at = now_iso()
    identity_rows = build_identity_rows(candidates, enrichment_map, enriched_at)
    retry_rows = build_retry_rows(
        candidates,
        enrichment_map,
        retry_base_minutes=args.retry_base_minutes,
    )

    success_count = len(identity_rows)
    fail_count = len(candidates) - success_count
    print(
        f"[INFO] identity 보강 완료: 성공 {success_count}건 / 실패 {fail_count}건 "
        f"(limit={len(candidates)})"
    )

    if args.dry_run:
        print(
            json.dumps(
                {
                    "seedCandidates": len(seed_candidates),
                    "seedReady": len(seed_rows),
                    "candidates": len(candidates),
                    "success": success_count,
                    "fail": fail_count,
                },
                ensure_ascii=False,
            )
        )
        return

    merged_identity = merge_rows(client, IDENTITY_TABLE_ID, identity_rows)
    merged_retry = merge_rows(client, RETRY_TABLE_ID, retry_rows)
    print(
        json.dumps(
            {
                "seedCandidates": len(seed_candidates),
                "seedUpserted": len(seed_rows),
                "candidates": len(candidates),
                "identityUpserted": merged_identity,
                "retryUpserted": merged_retry,
                "success": success_count,
                "fail": fail_count,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
