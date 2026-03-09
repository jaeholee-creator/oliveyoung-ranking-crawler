#!/usr/bin/env python3
"""수집된 올리브영 랭킹 JSON을 BigQuery에 적재하는 스크립트"""

import json
import sys
import os
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
TABLE_ID = "oliveyoung_ranking_history"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)


def load_to_bigquery(json_path: str) -> int:
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)

    with open(json_path) as f:
        rows = json.load(f)

    extra_fields = [
        "item_id", "og_url", "og_image_url", "eg_item_url",
        "goods_type_code", "goods_section_code", "trade_code", "delivery_policy_number",
        "online_brand_code", "online_brand_name", "online_brand_eng_name",
        "brand_code", "supplier_code", "supplier_name",
        "status_code", "status_name", "sold_out_flag",
        "registered_at", "modified_at", "display_start_at", "display_end_at",
        "standard_category_upper_code", "standard_category_upper_name",
        "standard_category_middle_code", "standard_category_middle_name",
        "standard_category_lower_code", "standard_category_lower_name",
        "display_category_upper_number", "display_category_upper_name",
        "display_category_middle_number", "display_category_middle_name",
        "display_category_lower_number", "display_category_lower_name",
        "display_category_leaf_number", "display_category_leaf_name",
        "option_number", "option_name", "standard_code", "option_image_url",
        "qna_count", "description_type_code", "description_image_count",
        "description_image_urls_json", "detail_meta_json", "extra_data_json",
    ]

    bq_rows = [
        ({
            "collected_at": r["collected_at_utc"],
            "category_code": r["category_code"],
            "category_name": r["category_name"],
            "rank": r["rank"],
            "goods_no": r["goods_no"],
            "detail_disp_cat_no": r["detail_disp_cat_no"],
            "detail_url": r["detail_url"],
            "brand_name": r["brand_name"],
            "product_name": r["product_name"],
            "original_price": r["original_price"],
            "discount_price": r["discount_price"],
            "discount_rate": r["discount_rate"],
            "ranking_tags": r["ranking_tags"],
            "review_count": r.get("review_count"),
            "rating": r.get("rating"),
        } | {field: r.get(field) for field in extra_fields})
        for r in rows
    ]

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_json(bq_rows, FULL_TABLE_ID, job_config=job_config)
    job.result()

    print(f"BigQuery 적재 완료: {job.output_rows}행 -> {FULL_TABLE_ID}")
    return job.output_rows


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_to_bigquery.py <ranking_rows.json>")
        sys.exit(1)
    load_to_bigquery(sys.argv[1])
