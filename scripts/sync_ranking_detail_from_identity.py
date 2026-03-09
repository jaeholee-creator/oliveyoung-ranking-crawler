#!/usr/bin/env python3
"""최근 랭킹 행에 identity 상세 컬럼을 반영한다."""

from __future__ import annotations

import argparse
import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
RANKING_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_ranking_history"
IDENTITY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_product_details"

KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)

DETAIL_FIELDS = [
    "item_id",
    "og_url",
    "og_image_url",
    "eg_item_url",
    "goods_type_code",
    "goods_section_code",
    "trade_code",
    "delivery_policy_number",
    "online_brand_code",
    "online_brand_name",
    "online_brand_eng_name",
    "brand_code",
    "supplier_code",
    "supplier_name",
    "status_code",
    "status_name",
    "sold_out_flag",
    "registered_at",
    "modified_at",
    "display_start_at",
    "display_end_at",
    "standard_category_upper_code",
    "standard_category_upper_name",
    "standard_category_middle_code",
    "standard_category_middle_name",
    "standard_category_lower_code",
    "standard_category_lower_name",
    "display_category_upper_number",
    "display_category_upper_name",
    "display_category_middle_number",
    "display_category_middle_name",
    "display_category_lower_number",
    "display_category_lower_name",
    "display_category_leaf_number",
    "display_category_leaf_name",
    "option_number",
    "option_name",
    "standard_code",
    "option_image_url",
    "qna_count",
    "description_type_code",
    "description_image_count",
    "description_image_urls_json",
    "detail_meta_json",
    "extra_data_json",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="최근 랭킹 row에 identity 상세 컬럼 반영")
    parser.add_argument("--lookback-hours", type=int, default=6, help="업데이트 대상 랭킹 lookback 시간")
    parser.add_argument("--dry-run", action="store_true", default=False, help="업데이트 없이 대상 row 수만 확인")
    return parser.parse_args()


def get_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def get_target_counts(client: bigquery.Client, lookback_hours: int) -> dict[str, int]:
    query = f"""
    SELECT
      COUNT(*) AS target_rows,
      COUNT(DISTINCT r.goods_no) AS target_goods
    FROM `{RANKING_TABLE_ID}` r
    JOIN `{IDENTITY_TABLE_ID}` i
      ON r.goods_no = i.goods_no
    WHERE r.collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours)]
    )
    row = next(iter(client.query(query, job_config=job_config).result()), None)
    if row is None:
        return {"target_rows": 0, "target_goods": 0}
    return {
        "target_rows": int(row["target_rows"] or 0),
        "target_goods": int(row["target_goods"] or 0),
    }


def sync_recent_rows(client: bigquery.Client, lookback_hours: int) -> int:
    set_clause = ",\n      ".join(f"{field} = i.{field}" for field in DETAIL_FIELDS)
    query = f"""
    UPDATE `{RANKING_TABLE_ID}` AS r
    SET
      {set_clause}
    FROM `{IDENTITY_TABLE_ID}` AS i
    WHERE r.goods_no = i.goods_no
      AND r.collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours)]
    )
    job = client.query(query, job_config=job_config)
    job.result()
    return int(job.num_dml_affected_rows or 0)


def main() -> None:
    args = parse_args()
    client = get_client()
    counts = get_target_counts(client, args.lookback_hours)

    if args.dry_run:
        print(json.dumps({"lookbackHours": args.lookback_hours, **counts}, ensure_ascii=False))
        return

    affected_rows = sync_recent_rows(client, args.lookback_hours)
    print(
        json.dumps(
            {
                "lookbackHours": args.lookback_hours,
                **counts,
                "affectedRows": affected_rows,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
