#!/usr/bin/env python3
"""
브랜드별 올리브영 랭킹 변동 리포트

BigQuery에서 최신 배치와 직전 배치를 비교하여
카테고리/제품별 시간별 순위 변동을 Slack으로 발송합니다.

사용법:
    python brand_ranking_report.py --brand 바이오던스
    python brand_ranking_report.py --brand 바이오던스 --channel C0ACH02BLG5
"""

import argparse
import json
import os
import sys
import urllib.request

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "member-378109"
DATASET_ID = "jaeho"
TABLE_ID = "oliveyoung_ranking"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.expanduser("~/Downloads/target-378109-7f1aa3e0dc9a.json"),
)
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ACH02BLG5")


CATEGORY_EMOJI = {
    "전체": "🏆",
    "마스크팩": "📦",
    "클렌징": "🧴",
    "스킨케어": "✨",
    "선케어": "☀️",
    "메이크업": "💄",
    "헤어케어": "💇",
    "바디케어": "🛁",
    "향수": "🌸",
    "건강식품": "💊",
    "푸드": "🍎",
    "홈리빙/가전": "🏠",
}


def get_bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def fetch_ranking_comparison(client: bigquery.Client, brand_name: str) -> dict:
    """최신 배치 vs 직전 배치 랭킹 비교 쿼리"""
    query = f"""
    WITH batches AS (
      -- 15분 이상 간격인 배치만 독립 배치로 인정
      SELECT DISTINCT
        TIMESTAMP_TRUNC(collected_at, MINUTE) AS batch_min
      FROM `{FULL_TABLE_ID}`
      WHERE brand_name = @brand_name
    ),
    ranked_batches AS (
      SELECT
        batch_min,
        ROW_NUMBER() OVER (ORDER BY batch_min DESC) AS rn
      FROM (
        SELECT
          batch_min,
          LAG(batch_min) OVER (ORDER BY batch_min) AS prev_batch_min
        FROM batches
      )
      WHERE prev_batch_min IS NULL
         OR TIMESTAMP_DIFF(batch_min, prev_batch_min, MINUTE) > 15
    ),
    latest_batch AS (SELECT batch_min FROM ranked_batches WHERE rn = 1),
    prev_batch    AS (SELECT batch_min FROM ranked_batches WHERE rn = 2),

    latest AS (
      SELECT category_name, goods_no, product_name, rank
      FROM `{FULL_TABLE_ID}`
      WHERE brand_name = @brand_name
        AND TIMESTAMP_TRUNC(collected_at, MINUTE) = (SELECT batch_min FROM latest_batch)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY category_name, goods_no ORDER BY collected_at DESC) = 1
    ),
    prev AS (
      SELECT category_name, goods_no, rank
      FROM `{FULL_TABLE_ID}`
      WHERE brand_name = @brand_name
        AND TIMESTAMP_TRUNC(collected_at, MINUTE) = (SELECT batch_min FROM prev_batch)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY category_name, goods_no ORDER BY collected_at DESC) = 1
    )

    SELECT
      l.category_name,
      l.product_name,
      l.rank                  AS current_rank,
      p.rank                  AS prev_rank,
      COALESCE(p.rank - l.rank, 0) AS rank_change,
      p.rank IS NULL          AS is_new,
      FORMAT_TIMESTAMP('%m/%d %H:%M', (SELECT batch_min FROM latest_batch), 'Asia/Seoul') AS current_time,
      FORMAT_TIMESTAMP('%H:%M', (SELECT batch_min FROM prev_batch),   'Asia/Seoul') AS prev_time
    FROM latest l
    LEFT JOIN prev p ON l.category_name = p.category_name AND l.goods_no = p.goods_no
    ORDER BY l.category_name, l.rank
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("brand_name", "STRING", brand_name),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        return {}

    current_time = rows[0]["current_time"]
    prev_time = rows[0]["prev_time"]

    categories: dict[str, list] = {}
    for row in rows:
        cat = row["category_name"]
        categories.setdefault(cat, []).append(row)

    return {
        "current_time": current_time,
        "prev_time": prev_time,
        "categories": categories,
    }


def format_change(row) -> str:
    if row["is_new"]:
        return "🆕"
    diff = row["rank_change"]
    if diff > 0:
        return f"▲{diff}"
    if diff < 0:
        return f"▼{abs(diff)}"
    return "―"


def build_slack_blocks(brand_name: str, data: dict) -> list:
    current_time = data["current_time"]
    prev_time = data["prev_time"]
    categories = data["categories"]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🌿 올리브영 {brand_name} 랭킹 변동 리포트",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"기준: *{current_time} KST* | 비교: *{prev_time} KST* (직전 배치)",
                }
            ],
        },
        {"type": "divider"},
    ]

    for cat_name, items in categories.items():
        emoji = CATEGORY_EMOJI.get(cat_name, "📂")

        lines = []
        for row in items:
            change = format_change(row)
            prev_info = f"_(전: {row['prev_rank']}위)_" if not row["is_new"] and row["rank_change"] != 0 else ""
            new_badge = "_신규 진입_" if row["is_new"] else ""
            product = row["product_name"]
            if len(product) > 35:
                product = product[:33] + "…"
            line = f"*{row['current_rank']}위*  {change}  {product}  {prev_info}{new_badge}"
            lines.append(line.strip())

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{emoji} {cat_name} 카테고리*\n" + "\n".join(lines),
                },
            }
        )
        blocks.append({"type": "divider"})

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "▲상승 | ▼하락 | 🆕신규진입 | ―변동없음 | 출처: 올리브영 판매랭킹",
                }
            ],
        }
    )

    return blocks


def send_slack(blocks: list, brand_name: str, channel: str, token: str) -> None:
    payload = json.dumps(
        {
            "channel": channel,
            "blocks": blocks,
            "text": f"올리브영 {brand_name} 랭킹 변동 리포트",
        },
        ensure_ascii=False,
    ).encode("utf-8")

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
    print(f"Slack 발송 완료: channel={channel}")


def main():
    parser = argparse.ArgumentParser(description="올리브영 브랜드 랭킹 변동 리포트")
    parser.add_argument("--brand", default="바이오던스", help="조회할 브랜드명")
    parser.add_argument("--channel", default=SLACK_CHANNEL_ID, help="Slack 채널 ID")
    parser.add_argument("--token", default=SLACK_BOT_TOKEN, help="Slack Bot Token")
    parser.add_argument("--dry-run", action="store_true", help="Slack 발송 없이 콘솔 출력만")
    args = parser.parse_args()

    print(f"[{args.brand}] BigQuery 랭킹 비교 조회 중...")
    client = get_bq_client()
    data = fetch_ranking_comparison(client, args.brand)

    if not data:
        print(f"데이터 없음: brand_name='{args.brand}'")
        sys.exit(1)

    print(f"기준: {data['current_time']} KST | 비교: {data['prev_time']} KST")
    for cat, items in data["categories"].items():
        print(f"\n[{cat}]")
        for row in items:
            change = format_change(row)
            print(f"  {row['current_rank']}위 {change}  {row['product_name']}")

    if args.dry_run:
        print("\n[dry-run] Slack 발송 스킵")
        return

    if not args.token:
        print("SLACK_BOT_TOKEN이 설정되지 않아 발송을 스킵합니다.", file=sys.stderr)
        sys.exit(1)

    blocks = build_slack_blocks(args.brand, data)
    send_slack(blocks, args.brand, args.channel, args.token)


if __name__ == "__main__":
    main()
