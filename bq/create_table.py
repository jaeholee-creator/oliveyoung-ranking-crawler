#!/usr/bin/env python3
"""BigQuery 테이블 생성 스크립트 - 올리브영 랭킹 데이터용"""

import argparse
import os
from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
TABLE_ID = "oliveyoung_ranking_history"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
TABLE_DESCRIPTION = "올리브영 판매 랭킹 히스토리 데이터 (매시 append 적재)"


def get_schema():
    return [
        bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED",
                             description="수집 시각 (UTC)"),
        bigquery.SchemaField("category_code", "STRING", mode="REQUIRED",
                             description="카테고리 코드 (예: ALL, 10000010001)"),
        bigquery.SchemaField("category_name", "STRING", mode="REQUIRED",
                             description="카테고리 이름 (예: 전체, 스킨케어)"),
        bigquery.SchemaField("rank", "INT64", mode="NULLABLE",
                             description="판매 랭킹 순위 (1~100)"),
        bigquery.SchemaField("goods_no", "STRING", mode="REQUIRED",
                             description="상품 고유번호"),
        bigquery.SchemaField("detail_disp_cat_no", "STRING", mode="NULLABLE",
                             description="상품 상세 전시 카테고리 번호"),
        bigquery.SchemaField("detail_url", "STRING", mode="NULLABLE",
                             description="상품 상세 페이지 URL"),
        bigquery.SchemaField("brand_name", "STRING", mode="NULLABLE",
                             description="브랜드명"),
        bigquery.SchemaField("product_name", "STRING", mode="NULLABLE",
                             description="상품명"),
        bigquery.SchemaField("original_price", "INT64", mode="NULLABLE",
                             description="원가 (원)"),
        bigquery.SchemaField("discount_price", "INT64", mode="NULLABLE",
                             description="할인가 (원)"),
        bigquery.SchemaField("discount_rate", "INT64", mode="NULLABLE",
                             description="할인율 (%)"),
        bigquery.SchemaField("ranking_tags", "STRING", mode="NULLABLE",
                             description="랭킹 태그 (쉼표 구분: 세일,쿠폰,증정,오늘드림 등)"),
        bigquery.SchemaField("review_count", "INT64", mode="NULLABLE",
                             description="리뷰 수"),
        bigquery.SchemaField("rating", "FLOAT64", mode="NULLABLE",
                             description="평균 평점 (1~5)"),
        bigquery.SchemaField("item_id", "STRING", mode="NULLABLE",
                             description="상품의 숫자형 내부 ID (eg:itemId / standardCode 계열)"),
        bigquery.SchemaField("og_url", "STRING", mode="NULLABLE",
                             description="상품 상세 OG URL"),
        bigquery.SchemaField("og_image_url", "STRING", mode="NULLABLE",
                             description="상품 상세 OG 이미지 URL"),
        bigquery.SchemaField("eg_item_url", "STRING", mode="NULLABLE",
                             description="상품 메타 item URL"),
        bigquery.SchemaField("goods_type_code", "STRING", mode="NULLABLE",
                             description="상품 타입 코드"),
        bigquery.SchemaField("goods_section_code", "STRING", mode="NULLABLE",
                             description="상품 섹션 코드"),
        bigquery.SchemaField("trade_code", "STRING", mode="NULLABLE",
                             description="거래 코드"),
        bigquery.SchemaField("delivery_policy_number", "STRING", mode="NULLABLE",
                             description="배송 정책 번호"),
        bigquery.SchemaField("online_brand_code", "STRING", mode="NULLABLE",
                             description="온라인 브랜드 코드"),
        bigquery.SchemaField("online_brand_name", "STRING", mode="NULLABLE",
                             description="온라인 브랜드명"),
        bigquery.SchemaField("online_brand_eng_name", "STRING", mode="NULLABLE",
                             description="온라인 브랜드 영문명"),
        bigquery.SchemaField("brand_code", "STRING", mode="NULLABLE",
                             description="브랜드 코드"),
        bigquery.SchemaField("supplier_code", "STRING", mode="NULLABLE",
                             description="공급사 코드"),
        bigquery.SchemaField("supplier_name", "STRING", mode="NULLABLE",
                             description="공급사명"),
        bigquery.SchemaField("status_code", "STRING", mode="NULLABLE",
                             description="상품 상태 코드"),
        bigquery.SchemaField("status_name", "STRING", mode="NULLABLE",
                             description="상품 상태명"),
        bigquery.SchemaField("sold_out_flag", "BOOL", mode="NULLABLE",
                             description="품절 여부"),
        bigquery.SchemaField("registered_at", "TIMESTAMP", mode="NULLABLE",
                             description="상품 등록 시각"),
        bigquery.SchemaField("modified_at", "TIMESTAMP", mode="NULLABLE",
                             description="상품 수정 시각"),
        bigquery.SchemaField("display_start_at", "TIMESTAMP", mode="NULLABLE",
                             description="상품 노출 시작 시각"),
        bigquery.SchemaField("display_end_at", "TIMESTAMP", mode="NULLABLE",
                             description="상품 노출 종료 시각"),
        bigquery.SchemaField("standard_category_upper_code", "STRING", mode="NULLABLE",
                             description="표준 카테고리 상위 코드"),
        bigquery.SchemaField("standard_category_upper_name", "STRING", mode="NULLABLE",
                             description="표준 카테고리 상위명"),
        bigquery.SchemaField("standard_category_middle_code", "STRING", mode="NULLABLE",
                             description="표준 카테고리 중간 코드"),
        bigquery.SchemaField("standard_category_middle_name", "STRING", mode="NULLABLE",
                             description="표준 카테고리 중간명"),
        bigquery.SchemaField("standard_category_lower_code", "STRING", mode="NULLABLE",
                             description="표준 카테고리 하위 코드"),
        bigquery.SchemaField("standard_category_lower_name", "STRING", mode="NULLABLE",
                             description="표준 카테고리 하위명"),
        bigquery.SchemaField("display_category_upper_number", "STRING", mode="NULLABLE",
                             description="전시 카테고리 상위 번호"),
        bigquery.SchemaField("display_category_upper_name", "STRING", mode="NULLABLE",
                             description="전시 카테고리 상위명"),
        bigquery.SchemaField("display_category_middle_number", "STRING", mode="NULLABLE",
                             description="전시 카테고리 중간 번호"),
        bigquery.SchemaField("display_category_middle_name", "STRING", mode="NULLABLE",
                             description="전시 카테고리 중간명"),
        bigquery.SchemaField("display_category_lower_number", "STRING", mode="NULLABLE",
                             description="전시 카테고리 하위 번호"),
        bigquery.SchemaField("display_category_lower_name", "STRING", mode="NULLABLE",
                             description="전시 카테고리 하위명"),
        bigquery.SchemaField("display_category_leaf_number", "STRING", mode="NULLABLE",
                             description="전시 카테고리 리프 번호"),
        bigquery.SchemaField("display_category_leaf_name", "STRING", mode="NULLABLE",
                             description="전시 카테고리 리프명"),
        bigquery.SchemaField("option_number", "STRING", mode="NULLABLE",
                             description="옵션 번호"),
        bigquery.SchemaField("option_name", "STRING", mode="NULLABLE",
                             description="옵션명"),
        bigquery.SchemaField("standard_code", "STRING", mode="NULLABLE",
                             description="표준 코드 / 바코드 계열 코드"),
        bigquery.SchemaField("option_image_url", "STRING", mode="NULLABLE",
                             description="옵션 대표 이미지 URL"),
        bigquery.SchemaField("qna_count", "INT64", mode="NULLABLE",
                             description="상품 QnA 수"),
        bigquery.SchemaField("description_type_code", "STRING", mode="NULLABLE",
                             description="상세 설명 타입 코드"),
        bigquery.SchemaField("description_image_count", "INT64", mode="NULLABLE",
                             description="상세 설명 이미지 개수"),
        bigquery.SchemaField("description_image_urls_json", "STRING", mode="NULLABLE",
                             description="상세 설명 이미지 URL 배열(JSON)"),
        bigquery.SchemaField("detail_meta_json", "STRING", mode="NULLABLE",
                             description="상세 HTML/RSC에서 추출한 메타 JSON"),
        bigquery.SchemaField("extra_data_json", "STRING", mode="NULLABLE",
                             description="/goods/api/v1/extra 응답 JSON"),
    ]


def create_table():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)

    # 데이터셋 존재 확인
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        ds = client.get_dataset(dataset_ref)
        print(f"데이터셋 '{DATASET_ID}' 확인 완료 (location: {ds.location})")
    except Exception as e:
        print(f"데이터셋 '{DATASET_ID}' 조회 실패: {e}")
        print("데이터셋 생성을 시도합니다...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast3"
        dataset.description = "Jaeho 개인 분석용 데이터셋"
        client.create_dataset(dataset)
        print(f"데이터셋 '{DATASET_ID}' 생성 완료.")

    # 테이블 생성
    table_ref = dataset_ref.table(TABLE_ID)
    table = bigquery.Table(table_ref, schema=get_schema())

    # 파티셔닝: collected_at 기준 일별 파티션 (비용 최적화)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="collected_at",
    )

    # 클러스터링: 자주 필터링하는 컬럼 기준
    table.clustering_fields = ["category_code", "brand_name"]

    table.description = TABLE_DESCRIPTION

    try:
        created = client.create_table(table)
        print(f"테이블 생성 완료: {created.full_table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"테이블 '{FULL_TABLE_ID}' 이미 존재합니다.")
        else:
            raise

    # 스키마 정보 출력
    print("\n=== 테이블 스키마 ===")
    for field in get_schema():
        print(f"  {field.name:25s} {field.field_type:10s} {field.mode:10s} | {field.description}")


def _sync_schema_metadata(
    client: bigquery.Client,
    full_table_id: str,
    expected_schema: list[bigquery.SchemaField],
    *,
    table_description: str,
) -> None:
    table = client.get_table(full_table_id)
    expected_by_name = {field.name: field for field in expected_schema}
    updated_schema = [expected_by_name.get(field.name, field) for field in table.schema]

    schema_changed = any(
        current.description != updated.description
        for current, updated in zip(table.schema, updated_schema, strict=False)
    ) or len(table.schema) != len(updated_schema)
    if schema_changed:
        table.schema = updated_schema
        client.update_table(table, ["schema"])
        print("스키마 설명 동기화 완료")

    table = client.get_table(full_table_id)
    if table.description != table_description:
        table.description = table_description
        client.update_table(table, ["description"])
        print("테이블 설명 동기화 완료")


def migrate_schema(*, prune_extra_columns: bool = False):
    """기존 테이블 스키마를 기대 스키마에 맞춘다."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    table = client.get_table(FULL_TABLE_ID)
    expected_schema = get_schema()
    expected_names = {field.name for field in expected_schema}
    existing_names = {field.name for field in table.schema}
    new_fields = [field for field in expected_schema if field.name not in existing_names]
    if new_fields:
        table.schema = list(table.schema) + new_fields
        client.update_table(table, ["schema"])
        print(f"스키마 업데이트: {[f.name for f in new_fields]} 추가됨")
    else:
        print("스키마 변경 없음")

    if prune_extra_columns:
        extra_fields = [
            field.name
            for field in client.get_table(FULL_TABLE_ID).schema
            if field.name not in expected_names
        ]
        if not extra_fields:
            print("삭제할 불필요 컬럼 없음")
        else:
            for field_name in extra_fields:
                client.query(f"ALTER TABLE `{FULL_TABLE_ID}` DROP COLUMN {field_name}").result()
                print(f"불필요 컬럼 삭제: {field_name}")

    _sync_schema_metadata(
        client,
        FULL_TABLE_ID,
        expected_schema,
        table_description=TABLE_DESCRIPTION,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="올리브영 랭킹 BigQuery 테이블 관리")
    parser.add_argument("--migrate", action="store_true", help="기존 테이블에 새 컬럼 추가")
    parser.add_argument(
        "--prune-extra-columns",
        action="store_true",
        help="기대 스키마에 없는 컬럼을 BigQuery 테이블에서 제거",
    )
    args = parser.parse_args()

    if args.migrate:
        migrate_schema(prune_extra_columns=args.prune_extra_columns)
    else:
        create_table()
