#!/usr/bin/env python3
"""올리브영 랭킹 최종 조회/진단 BigQuery View 생성."""

from __future__ import annotations

import os

from google.cloud import bigquery

from create_product_identity_tables import get_identity_schema, get_retry_schema
from create_table import get_schema as get_ranking_schema

SERVICE_ACCOUNT_KEY = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
RANKING_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_ranking"
IDENTITY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_product_identity"
RETRY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_product_identity_retry"
FINAL_VIEW_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_ranking_final_view"
GAP_VIEW_ID = f"{PROJECT_ID}.{DATASET_ID}.oliveyoung_ranking_detail_gap_view"
FINAL_VIEW_DESCRIPTION = "올리브영 최신 논리 배치 기준 랭킹+상세 통합 조회 뷰"
GAP_VIEW_DESCRIPTION = "올리브영 최신 논리 배치 기준 상세 누락 상품 진단 뷰"

DETAIL_ENRICHED_EXPR = """
(
  COALESCE(r.item_id, i.item_id) IS NOT NULL
  OR COALESCE(r.standard_code, i.standard_code) IS NOT NULL
  OR COALESCE(r.detail_meta_json, i.detail_meta_json) IS NOT NULL
  OR COALESCE(r.description_image_urls_json, i.description_image_urls_json) IS NOT NULL
)
""".strip()

COMMON_CTES = f"""
WITH batch_minutes AS (
  SELECT DISTINCT TIMESTAMP_TRUNC(collected_at, MINUTE) AS batch_min
  FROM `{RANKING_TABLE_ID}`
),
logical_batches AS (
  SELECT
    batch_min,
    SUM(
      CASE
        WHEN prev_batch_min IS NULL OR TIMESTAMP_DIFF(batch_min, prev_batch_min, MINUTE) > 15 THEN 1
        ELSE 0
      END
    ) OVER (ORDER BY batch_min) AS batch_group
  FROM (
    SELECT
      batch_min,
      LAG(batch_min) OVER (ORDER BY batch_min) AS prev_batch_min
    FROM batch_minutes
  )
),
latest_batch_group AS (
  SELECT MAX(batch_group) AS batch_group
  FROM logical_batches
),
latest_batch_bounds AS (
  SELECT
    MIN(batch_min) AS batch_started_at,
    MAX(batch_min) AS batch_ended_at
  FROM logical_batches
  WHERE batch_group = (SELECT batch_group FROM latest_batch_group)
),
latest_batch_ranking AS (
  SELECT
    r.*,
    bounds.batch_started_at,
    bounds.batch_ended_at
  FROM `{RANKING_TABLE_ID}` r
  JOIN logical_batches b
    ON TIMESTAMP_TRUNC(r.collected_at, MINUTE) = b.batch_min
  CROSS JOIN latest_batch_bounds bounds
  WHERE b.batch_group = (SELECT batch_group FROM latest_batch_group)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.category_code, r.rank, r.goods_no
    ORDER BY r.collected_at DESC
  ) = 1
),
latest_identity AS (
  SELECT * EXCEPT (row_num)
  FROM (
    SELECT
      i.*,
      ROW_NUMBER() OVER (
        PARTITION BY goods_no
        ORDER BY COALESCE(last_enriched_at, first_enriched_at) DESC, last_seen_at DESC
      ) AS row_num
    FROM `{IDENTITY_TABLE_ID}` i
  )
  WHERE row_num = 1
),
latest_retry AS (
  SELECT * EXCEPT (row_num)
  FROM (
    SELECT
      rr.*,
      ROW_NUMBER() OVER (
        PARTITION BY goods_no
        ORDER BY COALESCE(last_attempt_at, first_queued_at) DESC, next_retry_at DESC
      ) AS row_num
    FROM `{RETRY_TABLE_ID}` rr
  )
  WHERE row_num = 1
),
joined AS (
  SELECT
    r.batch_started_at,
    r.batch_ended_at,
    r.collected_at,
    r.category_code,
    r.category_name,
    r.rank,
    r.goods_no,
    r.detail_disp_cat_no,
    r.detail_url,
    r.brand_name,
    r.product_name,
    r.original_price,
    r.discount_price,
    r.discount_rate,
    r.ranking_tags,
    r.review_count,
    r.rating,
    COALESCE(r.item_id, i.item_id) AS item_id,
    COALESCE(r.og_url, i.og_url) AS og_url,
    COALESCE(r.og_image_url, i.og_image_url) AS og_image_url,
    COALESCE(r.eg_item_url, i.eg_item_url) AS eg_item_url,
    COALESCE(r.goods_type_code, i.goods_type_code) AS goods_type_code,
    COALESCE(r.goods_section_code, i.goods_section_code) AS goods_section_code,
    COALESCE(r.trade_code, i.trade_code) AS trade_code,
    COALESCE(r.delivery_policy_number, i.delivery_policy_number) AS delivery_policy_number,
    COALESCE(r.online_brand_code, i.online_brand_code) AS online_brand_code,
    COALESCE(r.online_brand_name, i.online_brand_name) AS online_brand_name,
    COALESCE(r.online_brand_eng_name, i.online_brand_eng_name) AS online_brand_eng_name,
    COALESCE(r.brand_code, i.brand_code) AS brand_code,
    COALESCE(r.supplier_code, i.supplier_code) AS supplier_code,
    COALESCE(r.supplier_name, i.supplier_name) AS supplier_name,
    COALESCE(r.status_code, i.status_code) AS status_code,
    COALESCE(r.status_name, i.status_name) AS status_name,
    COALESCE(r.sold_out_flag, i.sold_out_flag) AS sold_out_flag,
    COALESCE(r.registered_at, i.registered_at) AS registered_at,
    COALESCE(r.modified_at, i.modified_at) AS modified_at,
    COALESCE(r.display_start_at, i.display_start_at) AS display_start_at,
    COALESCE(r.display_end_at, i.display_end_at) AS display_end_at,
    COALESCE(r.standard_category_upper_code, i.standard_category_upper_code) AS standard_category_upper_code,
    COALESCE(r.standard_category_upper_name, i.standard_category_upper_name) AS standard_category_upper_name,
    COALESCE(r.standard_category_middle_code, i.standard_category_middle_code) AS standard_category_middle_code,
    COALESCE(r.standard_category_middle_name, i.standard_category_middle_name) AS standard_category_middle_name,
    COALESCE(r.standard_category_lower_code, i.standard_category_lower_code) AS standard_category_lower_code,
    COALESCE(r.standard_category_lower_name, i.standard_category_lower_name) AS standard_category_lower_name,
    COALESCE(r.display_category_upper_number, i.display_category_upper_number) AS display_category_upper_number,
    COALESCE(r.display_category_upper_name, i.display_category_upper_name) AS display_category_upper_name,
    COALESCE(r.display_category_middle_number, i.display_category_middle_number) AS display_category_middle_number,
    COALESCE(r.display_category_middle_name, i.display_category_middle_name) AS display_category_middle_name,
    COALESCE(r.display_category_lower_number, i.display_category_lower_number) AS display_category_lower_number,
    COALESCE(r.display_category_lower_name, i.display_category_lower_name) AS display_category_lower_name,
    COALESCE(r.display_category_leaf_number, i.display_category_leaf_number) AS display_category_leaf_number,
    COALESCE(r.display_category_leaf_name, i.display_category_leaf_name) AS display_category_leaf_name,
    COALESCE(r.option_number, i.option_number) AS option_number,
    COALESCE(r.option_name, i.option_name) AS option_name,
    COALESCE(r.standard_code, i.standard_code) AS standard_code,
    COALESCE(r.option_image_url, i.option_image_url) AS option_image_url,
    COALESCE(r.qna_count, i.qna_count) AS qna_count,
    COALESCE(r.description_type_code, i.description_type_code) AS description_type_code,
    COALESCE(r.description_image_count, i.description_image_count) AS description_image_count,
    COALESCE(r.description_image_urls_json, i.description_image_urls_json) AS description_image_urls_json,
    COALESCE(r.detail_meta_json, i.detail_meta_json) AS detail_meta_json,
    COALESCE(r.extra_data_json, i.extra_data_json) AS extra_data_json,
    i.identity_confidence,
    i.last_status AS identity_last_status,
    i.last_enriched_at,
    rr.retry_count,
    rr.last_status AS retry_last_status,
    rr.last_error AS retry_last_error,
    rr.last_attempt_at AS retry_last_attempt_at,
    rr.next_retry_at AS retry_next_retry_at,
    {DETAIL_ENRICHED_EXPR} AS detail_enriched
  FROM latest_batch_ranking r
  LEFT JOIN latest_identity i USING (goods_no)
  LEFT JOIN latest_retry rr USING (goods_no)
)
"""

FINAL_VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{FINAL_VIEW_ID}` AS
{COMMON_CTES}
SELECT
  *,
  CASE
    WHEN detail_enriched THEN 'ENRICHED'
    WHEN retry_last_status = 'PENDING' THEN 'RETRY_PENDING'
    WHEN retry_last_status = 'RESOLVED' THEN 'RESOLVED'
    ELSE 'MISSING'
  END AS detail_status
FROM joined
"""

GAP_VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{GAP_VIEW_ID}` AS
{COMMON_CTES}
SELECT
  *,
  CASE
    WHEN retry_last_status = 'PENDING' THEN 'RETRY_PENDING'
    WHEN retry_last_status = 'RESOLVED' THEN 'RESOLVED'
    ELSE 'MISSING'
  END AS detail_status
FROM joined
WHERE NOT detail_enriched
"""


def get_view_field_descriptions() -> dict[str, str]:
    descriptions = {field.name: field.description for field in get_ranking_schema()}
    descriptions.update(
        {
            "batch_started_at": "최신 논리 배치의 시작 시각(UTC, 분 단위)",
            "batch_ended_at": "최신 논리 배치의 종료 시각(UTC, 분 단위)",
            "identity_confidence": "상세 보강 결과의 식별자 신뢰도(HIGH/MEDIUM/LOW/NONE)",
            "identity_last_status": "상품 식별 보강 테이블 기준 마지막 상태",
            "last_enriched_at": "마지막 상세 보강 성공 시각",
            "retry_count": "상세 보강 재시도 누적 횟수",
            "retry_last_status": "재시도 큐 기준 마지막 상태",
            "retry_last_error": "재시도 큐 기준 마지막 실패 원인",
            "retry_last_attempt_at": "재시도 큐 기준 마지막 시도 시각",
            "retry_next_retry_at": "재시도 큐 기준 다음 재시도 예정 시각",
            "detail_enriched": "최종 조회 기준 상세 정보 존재 여부",
            "detail_status": "최종 조회 기준 상세 상태(ENRICHED/RETRY_PENDING/RESOLVED/MISSING)",
        }
    )

    identity_desc = {field.name: field.description for field in get_identity_schema()}
    retry_desc = {field.name: field.description for field in get_retry_schema()}
    descriptions["identity_confidence"] = identity_desc["identity_confidence"]
    descriptions["retry_count"] = retry_desc["retry_count"]
    return descriptions


def apply_view_metadata(
    client: bigquery.Client,
    view_id: str,
    *,
    description: str,
    field_descriptions: dict[str, str],
) -> None:
    table = client.get_table(view_id)
    updated_schema = [
        bigquery.SchemaField(
            field.name,
            field.field_type,
            mode=field.mode,
            description=field_descriptions.get(field.name),
            fields=field.fields,
        )
        for field in table.schema
    ]
    table.schema = updated_schema
    table.description = description
    client.update_table(table, ["schema", "description"])
    print(f"view 메타데이터 동기화 완료: {view_id}")


def get_client() -> bigquery.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    return bigquery.Client(project=PROJECT_ID)


def create_views() -> None:
    client = get_client()
    field_descriptions = get_view_field_descriptions()
    client.query(FINAL_VIEW_SQL).result()
    print(f"view 생성 완료: {FINAL_VIEW_ID}")
    apply_view_metadata(
        client,
        FINAL_VIEW_ID,
        description=FINAL_VIEW_DESCRIPTION,
        field_descriptions=field_descriptions,
    )
    client.query(GAP_VIEW_SQL).result()
    print(f"view 생성 완료: {GAP_VIEW_ID}")
    apply_view_metadata(
        client,
        GAP_VIEW_ID,
        description=GAP_VIEW_DESCRIPTION,
        field_descriptions=field_descriptions,
    )


if __name__ == "__main__":
    create_views()
