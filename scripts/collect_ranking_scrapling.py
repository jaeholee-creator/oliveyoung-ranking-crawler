#!/usr/bin/env python3
"""올리브영 랭킹 크롤러 - patchright (patched playwright) 기반.

Scrapling 의존성인 patchright를 직접 사용하여 Cloudflare를 우회하면서
카테고리 버튼 클릭 등 상호작용을 수행합니다.

Usage:
    python collect_ranking_scrapling.py [options]

Options:
    --url URL               올리브영 베스트 랭킹 페이지 URL
    --out-dir DIR           출력 디렉토리 (default: output/ranking_scrapling)
    --category-limit N      수집할 최대 카테고리 수 (default: 20)
    --category-wait-ms MS   카테고리 클릭 후 대기 시간 ms (default: 2500)
    --headed                헤드리스 모드 비활성화 (브라우저 창 표시)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin


def parse_args() -> argparse.Namespace:
    """CLI 인자 파싱."""
    parser = argparse.ArgumentParser(
        description="올리브영 랭킹 크롤러 (patchright 기반)",
    )
    parser.add_argument(
        "--url",
        default="https://www.oliveyoung.co.kr/store/main/getBestList.do",
        help="올리브영 베스트 랭킹 페이지 URL",
    )
    parser.add_argument(
        "--out-dir",
        default="output/ranking_scrapling",
        help="출력 디렉토리 (default: output/ranking_scrapling)",
    )
    parser.add_argument(
        "--category-limit",
        type=int,
        default=20,
        help="수집할 최대 카테고리 수 (default: 20)",
    )
    parser.add_argument(
        "--category-wait-ms",
        type=int,
        default=2500,
        help="카테고리 클릭 후 대기 시간 ms (default: 2500)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        default=False,
        help="헤드리스 모드 비활성화 (브라우저 창 표시)",
    )
    return parser.parse_args()


def now_iso() -> str:
    """현재 시각을 ISO 8601 UTC 문자열로 반환."""
    return datetime.now(timezone.utc).isoformat()


def to_int(value: Any) -> int | None:
    """문자열에서 숫자만 추출하여 정수로 변환. 변환 불가 시 None."""
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def escape_csv_field(value: Any) -> str:
    """CSV 필드를 이스케이프 처리."""
    if value is None:
        return ""
    s = str(value)
    if re.search(r'[",\n]', s):
        return f'"{s.replace(chr(34), chr(34) + chr(34))}"'
    return s


def to_csv(rows: list[dict], headers: list[str]) -> str:
    """행 목록과 헤더로 CSV 문자열 생성. Node.js 버전과 동일한 형식."""
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(escape_csv_field(row.get(h)) for h in headers))
    return "\n".join(lines) + "\n"


def extract_categories(page: Any) -> list[dict]:
    """페이지에서 카테고리 버튼 목록 추출."""
    return page.locator("div.common-menu button[data-ref-dispcatno]").evaluate_all(
        """nodes => nodes.map(node => {
            const rawCode = (node.getAttribute('data-ref-dispcatno') || '').trim();
            const rawName = (node.textContent || '').trim();
            return {
                rawCode,
                code: rawCode || 'ALL',
                name: rawName || '전체',
            };
        }).filter(item => item.name)"""
    )


def extract_items(page: Any) -> list[dict]:
    """현재 페이지에서 상품 정보를 추출."""
    return page.locator("div.prd_info").evaluate_all(
        """cards => {
            const out = [];
            for (const card of cards) {
                const thumb = card.querySelector('a.prd_thumb[href*="getGoodsDetail.do?goodsNo="]');
                if (!thumb) continue;
                const href = thumb.getAttribute('href') || '';
                let goodsNo = null;
                let dispCatNo = null;
                let rank = null;
                try {
                    const parsed = new URL(href, 'https://www.oliveyoung.co.kr');
                    goodsNo = parsed.searchParams.get('goodsNo');
                    dispCatNo = parsed.searchParams.get('dispCatNo');
                    const rankRaw = parsed.searchParams.get('t_number');
                    if (rankRaw && /^\\d+$/.test(rankRaw)) rank = Number(rankRaw);
                } catch (_) {}

                const brand = (card.querySelector('.tx_brand')?.textContent || '').trim();
                const productName = (card.querySelector('.tx_name')?.textContent || '').trim();
                const originalPriceText = (card.querySelector('.prd_price .tx_org .tx_num')?.textContent || '').trim();
                const discountPriceText = (card.querySelector('.prd_price .tx_cur .tx_num')?.textContent || '').trim();
                const tags = Array.from(card.querySelectorAll('.prd_flag .icon_flag'))
                    .map(node => (node.textContent || '').trim())
                    .filter(Boolean);

                out.push({
                    href,
                    goodsNo,
                    dispCatNo,
                    rank,
                    brand,
                    productName,
                    originalPriceText,
                    discountPriceText,
                    tags,
                });
            }
            return out;
        }"""
    )


def detect_challenge(page: Any) -> bool:
    """Cloudflare 챌린지 페이지가 감지되었는지 확인."""
    try:
        body_text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body_text = ""
    return bool(
        re.search(
            r"enable javascript and cookies to continue|잠시만 기다려 주세요|접속 정보를 확인 중이에요",
            body_text,
            re.IGNORECASE,
        )
    )


def normalize_prices(
    original_price_text: str,
    discount_price_text: str,
    tags: list[str],
) -> tuple[int | None, int | None, int | None]:
    """가격 정규화 및 할인율 계산. Node.js 버전과 동일한 로직."""
    original_price = to_int(original_price_text)
    discount_price = to_int(discount_price_text)
    discount_rate = None
    has_sale_tag = "세일" in tags

    if original_price is None and discount_price is not None and not has_sale_tag:
        # 원가 없고 할인가만 있고 세일 태그도 없으면 → 원가 = 할인가, 할인가 제거
        original_price = discount_price
        discount_price = None
    elif original_price is not None and discount_price is not None:
        if discount_price < original_price:
            discount_rate = round(((original_price - discount_price) * 100) / original_price)
        else:
            # 할인가가 원가 이상이면 할인 정보 무효
            discount_price = None
            discount_rate = None

    return original_price, discount_price, discount_rate


def build_detail_url(href: str) -> str | None:
    """상대 URL을 절대 URL로 변환. JS의 new URL(href, base).toString()과 동일."""
    if not href:
        return None
    return urljoin("https://www.oliveyoung.co.kr", href)


def run() -> None:
    """메인 크롤링 로직 실행."""
    args = parse_args()

    # patchright import (Scrapling 의존성)
    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        print(
            "ERROR: patchright 패키지가 설치되어 있지 않습니다.\n"
            "  설치: pip install patchright && python -m patchright install chromium",
            file=sys.stderr,
        )
        sys.exit(1)

    # 실행 ID 및 출력 디렉토리 생성
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z"
    out_dir = Path(os.getcwd()) / args.out_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    headless = not args.headed

    print(f"[INFO] 실행 ID: {run_id}")
    print(f"[INFO] 출력 디렉토리: {out_dir}")
    print(f"[INFO] 대상 URL: {args.url}")
    print(f"[INFO] 카테고리 제한: {args.category_limit}")
    print(f"[INFO] 헤드리스: {headless}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            locale="ko-KR",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        page = context.new_page()

        print(f"[INFO] 페이지 로딩 중: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(4000)

        # 카테고리 목록 추출
        categories = extract_categories(page)
        target_categories = categories[: max(0, args.category_limit)]
        print(f"[INFO] 발견된 카테고리: {len(categories)}개, 대상: {len(target_categories)}개")

        rows: list[dict] = []
        category_summaries: list[dict] = []

        for idx, category in enumerate(target_categories):
            cat_label = f"[{idx + 1}/{len(target_categories)}] {category['name']} ({category['code']})"
            print(f"[INFO] 카테고리 처리 중: {cat_label}")

            button_code = category.get("rawCode", "")
            locator = page.locator(f'button[data-ref-dispcatno="{button_code}"]').first
            locator.wait_for(timeout=15000)
            locator.click(timeout=15000)
            page.wait_for_timeout(args.category_wait_ms)

            # Cloudflare 챌린지 감지
            if detect_challenge(page):
                print(f"[WARN] Cloudflare 챌린지 감지됨! 크롤링 중단: {cat_label}")
                category_summaries.append(
                    {
                        "categoryCode": category["code"],
                        "categoryName": category["name"],
                        "challengeDetected": True,
                        "goodsCount": 0,
                    }
                )
                break

            # 상품 데이터 추출
            items = extract_items(page)

            # 중복 제거 (goodsNo 기준)
            dedup: dict[str, dict] = {}
            for item in items:
                goods_no = item.get("goodsNo")
                if not goods_no:
                    continue
                if goods_no not in dedup:
                    dedup[goods_no] = item

            # 랭킹 순서로 정렬
            normalized = sorted(
                dedup.values(),
                key=lambda x: x.get("rank") if isinstance(x.get("rank"), (int, float)) else 9999,
            )

            # 행 데이터 구성
            for item in normalized:
                original_price, discount_price, discount_rate = normalize_prices(
                    item.get("originalPriceText", ""),
                    item.get("discountPriceText", ""),
                    item.get("tags", []),
                )

                rows.append(
                    {
                        "collected_at_utc": now_iso(),
                        "category_code": category["code"],
                        "category_name": category["name"],
                        "rank": item.get("rank"),
                        "goods_no": item.get("goodsNo"),
                        "detail_disp_cat_no": item.get("dispCatNo"),
                        "detail_url": build_detail_url(item.get("href", "")),
                        "brand_name": item.get("brand", ""),
                        "product_name": item.get("productName", ""),
                        "original_price": original_price,
                        "discount_price": discount_price,
                        "discount_rate": discount_rate,
                        "ranking_tags": ",".join(item.get("tags", [])),
                    }
                )

            # 카테고리 요약 정보
            ranks = [
                item.get("rank")
                for item in normalized
                if isinstance(item.get("rank"), (int, float))
            ]
            category_summaries.append(
                {
                    "categoryCode": category["code"],
                    "categoryName": category["name"],
                    "challengeDetected": False,
                    "goodsCount": len(normalized),
                    "rankMin": min(ranks) if ranks else None,
                    "rankMax": max(ranks) if ranks else None,
                    "urlAfterClick": page.url,
                }
            )
            print(f"[INFO] 카테고리 완료: {cat_label} → {len(normalized)}개 상품")

        # --- 파일 출력 ---
        csv_headers = [
            "collected_at_utc",
            "category_code",
            "category_name",
            "rank",
            "goods_no",
            "detail_disp_cat_no",
            "detail_url",
            "brand_name",
            "product_name",
            "original_price",
            "discount_price",
            "discount_rate",
            "ranking_tags",
        ]

        csv_path = str(out_dir / "ranking_rows.csv")
        json_path = str(out_dir / "ranking_rows.json")
        summary_path = str(out_dir / "summary.json")

        # CSV 출력
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            f.write(to_csv(rows, csv_headers))

        # JSON 출력
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

        # Summary 출력
        summary = {
            "runId": run_id,
            "startedAt": now_iso(),
            "finishedAt": now_iso(),
            "targetUrl": args.url,
            "categoriesRequested": len(target_categories),
            "categoriesCaptured": len(category_summaries),
            "rows": len(rows),
            "categorySummaries": category_summaries,
            "files": {"csvPath": csv_path, "jsonPath": json_path},
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # 브라우저 종료
        context.close()
        browser.close()

    # 결과 출력
    result = {
        "outDir": str(out_dir),
        "csvPath": csv_path,
        "jsonPath": json_path,
        "summaryPath": summary_path,
        "rows": len(rows),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    run()
