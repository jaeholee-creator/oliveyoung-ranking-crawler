#!/usr/bin/env python3
"""BigQuery 테이블 생성 스크립트 - 올리브영 랭킹 데이터용"""

import os
from google.cloud import bigquery

SERVICE_ACCOUNT_KEY = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
TABLE_ID = "oliveyoung_ranking"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


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

    table.description = "올리브영 판매 랭킹 데이터 (30분 주기 수집)"

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


if __name__ == "__main__":
    create_table()
