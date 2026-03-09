#!/usr/bin/env python3
"""올리브영 상품 식별/재시도 BigQuery 테이블 생성 스크립트."""

from __future__ import annotations

import argparse
import os

from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
IDENTITY_TABLE_ID = "oliveyoung_product_identity"
RETRY_TABLE_ID = "oliveyoung_product_identity_retry"
FULL_IDENTITY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{IDENTITY_TABLE_ID}"
FULL_RETRY_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{RETRY_TABLE_ID}"
IDENTITY_TABLE_DESCRIPTION = "올리브영 상품 식별 정보 및 canonical_product_id 브리지"
RETRY_TABLE_DESCRIPTION = "올리브영 상품 식별 재시도 큐"


def get_identity_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("goods_no", "STRING", mode="REQUIRED", description="상품 번호"),
        bigquery.SchemaField("canonical_product_id", "STRING", mode="NULLABLE", description="연결용 대표 상품 ID"),
        bigquery.SchemaField("item_id", "STRING", mode="NULLABLE", description="상품 숫자형 내부 ID"),
        bigquery.SchemaField("standard_code", "STRING", mode="NULLABLE", description="표준 코드 / 바코드 계열"),
        bigquery.SchemaField("option_number", "STRING", mode="NULLABLE", description="옵션 번호"),
        bigquery.SchemaField("option_name", "STRING", mode="NULLABLE", description="옵션명"),
        bigquery.SchemaField("option_image_url", "STRING", mode="NULLABLE", description="옵션 대표 이미지 URL"),
        bigquery.SchemaField("source_detail_url", "STRING", mode="NULLABLE", description="상세 수집에 사용한 URL"),
        bigquery.SchemaField("brand_name", "STRING", mode="NULLABLE", description="랭킹 기준 최신 브랜드명"),
        bigquery.SchemaField("product_name", "STRING", mode="NULLABLE", description="랭킹 기준 최신 상품명"),
        bigquery.SchemaField("normalized_product_name", "STRING", mode="NULLABLE", description="프로모션 토큰 제거 후 정규화 상품명"),
        bigquery.SchemaField("brand_code", "STRING", mode="NULLABLE", description="브랜드 코드"),
        bigquery.SchemaField("supplier_code", "STRING", mode="NULLABLE", description="공급사 코드"),
        bigquery.SchemaField("supplier_name", "STRING", mode="NULLABLE", description="공급사명"),
        bigquery.SchemaField("online_brand_code", "STRING", mode="NULLABLE", description="온라인 브랜드 코드"),
        bigquery.SchemaField("online_brand_name", "STRING", mode="NULLABLE", description="온라인 브랜드명"),
        bigquery.SchemaField("online_brand_eng_name", "STRING", mode="NULLABLE", description="온라인 브랜드 영문명"),
        bigquery.SchemaField("goods_type_code", "STRING", mode="NULLABLE", description="상품 타입 코드"),
        bigquery.SchemaField("goods_section_code", "STRING", mode="NULLABLE", description="상품 섹션 코드"),
        bigquery.SchemaField("trade_code", "STRING", mode="NULLABLE", description="거래 코드"),
        bigquery.SchemaField("delivery_policy_number", "STRING", mode="NULLABLE", description="배송 정책 번호"),
        bigquery.SchemaField("status_code", "STRING", mode="NULLABLE", description="상품 상태 코드"),
        bigquery.SchemaField("status_name", "STRING", mode="NULLABLE", description="상품 상태명"),
        bigquery.SchemaField("sold_out_flag", "BOOL", mode="NULLABLE", description="품절 여부"),
        bigquery.SchemaField("registered_at", "TIMESTAMP", mode="NULLABLE", description="상품 등록 시각"),
        bigquery.SchemaField("modified_at", "TIMESTAMP", mode="NULLABLE", description="상품 수정 시각"),
        bigquery.SchemaField("display_start_at", "TIMESTAMP", mode="NULLABLE", description="노출 시작 시각"),
        bigquery.SchemaField("display_end_at", "TIMESTAMP", mode="NULLABLE", description="노출 종료 시각"),
        bigquery.SchemaField("standard_category_upper_code", "STRING", mode="NULLABLE", description="표준 카테고리 상위 코드"),
        bigquery.SchemaField("standard_category_upper_name", "STRING", mode="NULLABLE", description="표준 카테고리 상위명"),
        bigquery.SchemaField("standard_category_middle_code", "STRING", mode="NULLABLE", description="표준 카테고리 중간 코드"),
        bigquery.SchemaField("standard_category_middle_name", "STRING", mode="NULLABLE", description="표준 카테고리 중간명"),
        bigquery.SchemaField("standard_category_lower_code", "STRING", mode="NULLABLE", description="표준 카테고리 하위 코드"),
        bigquery.SchemaField("standard_category_lower_name", "STRING", mode="NULLABLE", description="표준 카테고리 하위명"),
        bigquery.SchemaField("display_category_upper_number", "STRING", mode="NULLABLE", description="전시 카테고리 상위 번호"),
        bigquery.SchemaField("display_category_upper_name", "STRING", mode="NULLABLE", description="전시 카테고리 상위명"),
        bigquery.SchemaField("display_category_middle_number", "STRING", mode="NULLABLE", description="전시 카테고리 중간 번호"),
        bigquery.SchemaField("display_category_middle_name", "STRING", mode="NULLABLE", description="전시 카테고리 중간명"),
        bigquery.SchemaField("display_category_lower_number", "STRING", mode="NULLABLE", description="전시 카테고리 하위 번호"),
        bigquery.SchemaField("display_category_lower_name", "STRING", mode="NULLABLE", description="전시 카테고리 하위명"),
        bigquery.SchemaField("display_category_leaf_number", "STRING", mode="NULLABLE", description="전시 카테고리 리프 번호"),
        bigquery.SchemaField("display_category_leaf_name", "STRING", mode="NULLABLE", description="전시 카테고리 리프명"),
        bigquery.SchemaField("og_url", "STRING", mode="NULLABLE", description="상품 OG URL"),
        bigquery.SchemaField("og_image_url", "STRING", mode="NULLABLE", description="상품 OG 이미지 URL"),
        bigquery.SchemaField("eg_item_url", "STRING", mode="NULLABLE", description="상품 item URL"),
        bigquery.SchemaField("qna_count", "INT64", mode="NULLABLE", description="상품 QnA 수"),
        bigquery.SchemaField("description_type_code", "STRING", mode="NULLABLE", description="상세 설명 타입 코드"),
        bigquery.SchemaField("description_image_count", "INT64", mode="NULLABLE", description="상세 설명 이미지 개수"),
        bigquery.SchemaField("description_image_urls_json", "STRING", mode="NULLABLE", description="상세 설명 이미지 URL 배열(JSON)"),
        bigquery.SchemaField("detail_meta_json", "STRING", mode="NULLABLE", description="상세 메타 JSON"),
        bigquery.SchemaField("extra_data_json", "STRING", mode="NULLABLE", description="extra API 응답 JSON"),
        bigquery.SchemaField("identity_confidence", "STRING", mode="NULLABLE", description="식별자 신뢰도"),
        bigquery.SchemaField("first_seen_at", "TIMESTAMP", mode="NULLABLE", description="랭킹에서 처음 본 시각"),
        bigquery.SchemaField("last_seen_at", "TIMESTAMP", mode="NULLABLE", description="랭킹에서 마지막으로 본 시각"),
        bigquery.SchemaField("first_enriched_at", "TIMESTAMP", mode="NULLABLE", description="처음 식별 보강 성공 시각"),
        bigquery.SchemaField("last_enriched_at", "TIMESTAMP", mode="NULLABLE", description="마지막 식별 보강 시각"),
        bigquery.SchemaField("last_status", "STRING", mode="NULLABLE", description="마지막 식별 보강 상태"),
    ]


def get_retry_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("goods_no", "STRING", mode="REQUIRED", description="상품 번호"),
        bigquery.SchemaField("source_detail_url", "STRING", mode="NULLABLE", description="재시도 대상 상세 URL"),
        bigquery.SchemaField("last_seen_at", "TIMESTAMP", mode="NULLABLE", description="랭킹에서 마지막으로 본 시각"),
        bigquery.SchemaField("first_queued_at", "TIMESTAMP", mode="NULLABLE", description="재시도 큐에 처음 들어간 시각"),
        bigquery.SchemaField("last_attempt_at", "TIMESTAMP", mode="NULLABLE", description="마지막 시도 시각"),
        bigquery.SchemaField("next_retry_at", "TIMESTAMP", mode="NULLABLE", description="다음 재시도 예정 시각"),
        bigquery.SchemaField("retry_count", "INT64", mode="NULLABLE", description="누적 재시도 횟수"),
        bigquery.SchemaField("last_status", "STRING", mode="NULLABLE", description="마지막 재시도 상태"),
        bigquery.SchemaField("last_error", "STRING", mode="NULLABLE", description="마지막 실패 원인"),
        bigquery.SchemaField("resolved_at", "TIMESTAMP", mode="NULLABLE", description="성공으로 해소된 시각"),
    ]


def ensure_dataset(client: bigquery.Client) -> bigquery.DatasetReference:
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast3"
        dataset.description = "Jaeho 개인 분석용 데이터셋"
        client.create_dataset(dataset)
    return dataset_ref


def ensure_table(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table_id: str,
    schema: list[bigquery.SchemaField],
    *,
    partition_field: str,
    clustering_fields: list[str],
    description: str,
) -> None:
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    table.clustering_fields = clustering_fields
    table.description = description

    try:
        created = client.create_table(table)
        print(f"테이블 생성 완료: {created.full_table_id}")
    except Exception as exc:
        if "Already Exists" in str(exc):
            print(f"테이블 '{full_table_id}' 이미 존재합니다.")
        else:
            raise


def migrate_table(
    client: bigquery.Client,
    full_table_id: str,
    schema: list[bigquery.SchemaField],
    *,
    table_description: str,
) -> None:
    table = client.get_table(full_table_id)
    existing_names = {field.name for field in table.schema}
    new_fields = [field for field in schema if field.name not in existing_names]
    if new_fields:
        table.schema = list(table.schema) + new_fields
        client.update_table(table, ["schema"])
        print(f"스키마 업데이트: {full_table_id} -> {[field.name for field in new_fields]}")
    else:
        print(f"스키마 변경 없음: {full_table_id}")

    table = client.get_table(full_table_id)
    expected_by_name = {field.name: field for field in schema}
    updated_schema = [expected_by_name.get(field.name, field) for field in table.schema]
    schema_changed = any(
        current.description != updated.description
        for current, updated in zip(table.schema, updated_schema, strict=False)
    ) or len(table.schema) != len(updated_schema)
    if schema_changed:
        table.schema = updated_schema
        client.update_table(table, ["schema"])
        print(f"스키마 설명 동기화: {full_table_id}")

    table = client.get_table(full_table_id)
    if table.description != table_description:
        table.description = table_description
        client.update_table(table, ["description"])
        print(f"테이블 설명 동기화: {full_table_id}")


def create_tables() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = ensure_dataset(client)

    ensure_table(
        client,
        dataset_ref,
        IDENTITY_TABLE_ID,
        get_identity_schema(),
        partition_field="last_enriched_at",
        clustering_fields=["canonical_product_id", "goods_no", "item_id"],
        description=IDENTITY_TABLE_DESCRIPTION,
    )
    ensure_table(
        client,
        dataset_ref,
        RETRY_TABLE_ID,
        get_retry_schema(),
        partition_field="next_retry_at",
        clustering_fields=["last_status", "goods_no"],
        description=RETRY_TABLE_DESCRIPTION,
    )


def migrate_tables() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    migrate_table(
        client,
        FULL_IDENTITY_TABLE_ID,
        get_identity_schema(),
        table_description=IDENTITY_TABLE_DESCRIPTION,
    )
    migrate_table(
        client,
        FULL_RETRY_TABLE_ID,
        get_retry_schema(),
        table_description=RETRY_TABLE_DESCRIPTION,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="올리브영 상품 식별용 BigQuery 테이블 관리")
    parser.add_argument("--migrate", action="store_true", help="기존 테이블에 새 컬럼 추가")
    args = parser.parse_args()

    if args.migrate:
        migrate_tables()
    else:
        create_tables()
