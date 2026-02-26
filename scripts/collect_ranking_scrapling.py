#!/usr/bin/env python3
"""올리브영 랭킹 크롤러 - patchright (patched playwright) 기반.

CF 우회 안정화:
- headless=False + Xvfb (서버) / headed (로컬)
- launch_persistent_context + channel="chrome"
- CF 챌린지 감지 시 대기 후 재시도 (최대 3회)
- 마우스 움직임/랜덤 딜레이로 human-like behavior

Usage:
    python collect_ranking_scrapling.py [options]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

MAX_CF_RETRIES = 3
CF_WAIT_SECONDS = 15


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="올리브영 랭킹 크롤러 (patchright)")
    parser.add_argument("--url", default="https://www.oliveyoung.co.kr/store/main/getBestList.do")
    parser.add_argument("--out-dir", default="output/ranking_scrapling")
    parser.add_argument("--category-limit", type=int, default=20)
    parser.add_argument("--category-wait-ms", type=int, default=2500)
    parser.add_argument("--headed", action="store_true", default=False)
    parser.add_argument("--min-rows", type=int, default=100, help="최소 수집 건수 (미달 시 exit 1)")
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def escape_csv_field(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    if re.search(r'[",\n]', s):
        return f'"{s.replace(chr(34), chr(34) + chr(34))}"'
    return s


def to_csv(rows: list[dict], headers: list[str]) -> str:
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(escape_csv_field(row.get(h)) for h in headers))
    return "\n".join(lines) + "\n"


def detect_challenge(page: Any) -> bool:
    """Cloudflare 챌린지 페이지 감지."""
    try:
        body_text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body_text = ""
    return bool(
        re.search(
            r"enable javascript and cookies to continue|잠시만 기다려 주세요|접속 정보를 확인 중이에요|Just a moment",
            body_text,
            re.IGNORECASE,
        )
    )


def wait_for_cf_resolution(page: Any, timeout_sec: int = CF_WAIT_SECONDS) -> bool:
    """CF 챌린지가 자동 해결될 때까지 대기. 해결되면 True."""
    print(f"[INFO] CF 챌린지 감지, {timeout_sec}초 대기 중...")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        time.sleep(2)
        if not detect_challenge(page):
            print("[INFO] CF 챌린지 해결됨!")
            return True
    print("[WARN] CF 챌린지 해결 타임아웃")
    return False


def simulate_human(page: Any) -> None:
    """마우스 움직임과 스크롤로 인간 행동 시뮬레이션."""
    try:
        page.mouse.move(random.randint(200, 800), random.randint(200, 500))
        time.sleep(random.uniform(0.3, 0.8))
        page.mouse.move(random.randint(300, 700), random.randint(100, 400))
        time.sleep(random.uniform(0.2, 0.5))
        page.evaluate("window.scrollBy(0, Math.floor(Math.random() * 300 + 100))")
        time.sleep(random.uniform(0.5, 1.0))
    except Exception:
        pass


def extract_categories(page: Any) -> list[dict]:
    return page.locator("div.common-menu button[data-ref-dispcatno]").evaluate_all(
        """nodes => nodes.map(node => {
            const rawCode = (node.getAttribute('data-ref-dispcatno') || '').trim();
            const rawName = (node.textContent || '').trim();
            return { rawCode, code: rawCode || 'ALL', name: rawName || '전체' };
        }).filter(item => item.name)"""
    )


def extract_items(page: Any) -> list[dict]:
    return page.locator("div.prd_info").evaluate_all(
        """cards => {
            const out = [];
            for (const card of cards) {
                const thumb = card.querySelector('a.prd_thumb[href*="getGoodsDetail.do?goodsNo="]');
                if (!thumb) continue;
                const href = thumb.getAttribute('href') || '';
                let goodsNo = null, dispCatNo = null, rank = null;
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
                    .map(node => (node.textContent || '').trim()).filter(Boolean);
                out.push({ href, goodsNo, dispCatNo, rank, brand, productName, originalPriceText, discountPriceText, tags });
            }
            return out;
        }"""
    )


def normalize_prices(
    original_price_text: str, discount_price_text: str, tags: list[str],
) -> tuple[int | None, int | None, int | None]:
    original_price = to_int(original_price_text)
    discount_price = to_int(discount_price_text)
    discount_rate = None
    has_sale_tag = "세일" in tags

    if original_price is None and discount_price is not None and not has_sale_tag:
        original_price = discount_price
        discount_price = None
    elif original_price is not None and discount_price is not None:
        if discount_price < original_price:
            discount_rate = round(((original_price - discount_price) * 100) / original_price)
        else:
            discount_price = None
            discount_rate = None

    return original_price, discount_price, discount_rate


def build_detail_url(href: str) -> str | None:
    if not href:
        return None
    return urljoin("https://www.oliveyoung.co.kr", href)


def run() -> None:
    args = parse_args()

    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: patchright 미설치. pip install patchright", file=sys.stderr)
        sys.exit(1)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z"
    out_dir = Path(os.getcwd()) / args.out_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # persistent context용 user_data_dir
    user_data_dir = Path(os.getcwd()) / ".browser_profile"
    user_data_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] 실행 ID: {run_id}")
    print(f"[INFO] 출력 디렉토리: {out_dir}")
    print(f"[INFO] 대상 URL: {args.url}")
    print(f"[INFO] 카테고리 제한: {args.category_limit}")
    print(f"[INFO] 최소 수집 건수: {args.min_rows}")

    rows: list[dict] = []
    category_summaries: list[dict] = []

    with sync_playwright() as pw:
        # persistent context: CF 쿠키(cf_clearance) 유지
        # channel="chrome"은 ARM64 Linux에서 미지원, Chromium 사용
        launch_kwargs = {
            "user_data_dir": str(user_data_dir),
            "headless": False,  # Xvfb 환경에서 headed 모드 사용
            "no_viewport": True,
            "locale": "ko-KR",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        }
        # x86 환경에서만 channel="chrome" 사용
        import platform
        if platform.machine() not in ("aarch64", "arm64"):
            launch_kwargs["channel"] = "chrome"
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
        page = context.new_page()

        # 초기 페이지 로드 + CF 챌린지 대기/재시도
        loaded = False
        for attempt in range(1, MAX_CF_RETRIES + 1):
            print(f"[INFO] 페이지 로딩 시도 {attempt}/{MAX_CF_RETRIES}: {args.url}")
            page.goto(args.url, wait_until="domcontentloaded", timeout=120000)
            time.sleep(random.uniform(3.0, 5.0))

            # 마우스 움직임으로 human-like
            simulate_human(page)

            if detect_challenge(page):
                resolved = wait_for_cf_resolution(page, CF_WAIT_SECONDS)
                if resolved:
                    loaded = True
                    break
                # 재시도: 페이지 새로고침
                print(f"[WARN] CF 챌린지 미해결, 재시도...")
                page.reload(wait_until="domcontentloaded", timeout=120000)
                time.sleep(random.uniform(5.0, 10.0))
                if not detect_challenge(page):
                    loaded = True
                    break
            else:
                loaded = True
                break

        if not loaded:
            print("[ERROR] CF 챌린지를 통과할 수 없음. 모든 재시도 실패.")
            _write_output(out_dir, rows, category_summaries, args, run_id)
            sys.exit(1)

        print("[INFO] 페이지 로드 성공!")
        simulate_human(page)

        # 카테고리 목록 추출
        categories = extract_categories(page)
        target_categories = categories[: max(0, args.category_limit)]
        print(f"[INFO] 발견된 카테고리: {len(categories)}개, 대상: {len(target_categories)}개")

        if not target_categories:
            print("[ERROR] 카테고리를 찾을 수 없음")
            _write_output(out_dir, rows, category_summaries, args, run_id)
            sys.exit(1)

        for idx, category in enumerate(target_categories):
            cat_label = f"[{idx + 1}/{len(target_categories)}] {category['name']} ({category['code']})"
            print(f"[INFO] 카테고리 처리 중: {cat_label}")

            button_code = category.get("rawCode", "")
            locator = page.locator(f'button[data-ref-dispcatno="{button_code}"]').first
            locator.wait_for(timeout=15000)

            # 클릭 전 랜덤 딜레이
            time.sleep(random.uniform(0.3, 0.8))
            locator.click(timeout=15000)

            # 카테고리 전환 대기 (랜덤화)
            wait_ms = args.category_wait_ms + random.randint(-300, 500)
            page.wait_for_timeout(max(1500, wait_ms))

            # CF 챌린지 감지 → 대기 후 재시도
            if detect_challenge(page):
                print(f"[WARN] 카테고리 전환 중 CF 챌린지 감지: {cat_label}")
                resolved = wait_for_cf_resolution(page, CF_WAIT_SECONDS)
                if not resolved:
                    category_summaries.append({
                        "categoryCode": category["code"],
                        "categoryName": category["name"],
                        "challengeDetected": True,
                        "goodsCount": 0,
                    })
                    # 한 번 더 시도
                    page.reload(wait_until="domcontentloaded", timeout=60000)
                    time.sleep(random.uniform(5.0, 8.0))
                    if detect_challenge(page):
                        print(f"[ERROR] CF 챌린지 지속, 남은 카테고리 스킵")
                        break
                    # 챌린지 해결됨 — 카테고리 버튼 다시 클릭
                    locator = page.locator(f'button[data-ref-dispcatno="{button_code}"]').first
                    locator.click(timeout=15000)
                    page.wait_for_timeout(args.category_wait_ms)

            # 상품 데이터 추출
            items = extract_items(page)

            # 중복 제거
            dedup: dict[str, dict] = {}
            for item in items:
                goods_no = item.get("goodsNo")
                if goods_no and goods_no not in dedup:
                    dedup[goods_no] = item

            normalized = sorted(
                dedup.values(),
                key=lambda x: x.get("rank") if isinstance(x.get("rank"), (int, float)) else 9999,
            )

            for item in normalized:
                original_price, discount_price, discount_rate = normalize_prices(
                    item.get("originalPriceText", ""),
                    item.get("discountPriceText", ""),
                    item.get("tags", []),
                )
                rows.append({
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
                })

            ranks = [
                item.get("rank") for item in normalized
                if isinstance(item.get("rank"), (int, float))
            ]
            category_summaries.append({
                "categoryCode": category["code"],
                "categoryName": category["name"],
                "challengeDetected": False,
                "goodsCount": len(normalized),
                "rankMin": min(ranks) if ranks else None,
                "rankMax": max(ranks) if ranks else None,
                "urlAfterClick": page.url,
            })
            print(f"[INFO] 카테고리 완료: {cat_label} → {len(normalized)}개 상품")

            # 카테고리 간 랜덤 딜레이
            if idx < len(target_categories) - 1:
                time.sleep(random.uniform(0.5, 1.5))

        page.close()
        context.close()

    _write_output(out_dir, rows, category_summaries, args, run_id)

    # 최소 건수 검증 → 미달 시 exit 1
    if len(rows) < args.min_rows:
        print(f"[ERROR] 수집 건수({len(rows)})가 최소 기준({args.min_rows})에 미달. 실패 처리.")
        sys.exit(1)

    print(f"[SUCCESS] 총 {len(rows)}건 수집 완료")


def _write_output(
    out_dir: Path,
    rows: list[dict],
    category_summaries: list[dict],
    args: argparse.Namespace,
    run_id: str,
) -> None:
    """파일 출력 (성공/실패 모두 기록)."""
    csv_headers = [
        "collected_at_utc", "category_code", "category_name", "rank",
        "goods_no", "detail_disp_cat_no", "detail_url", "brand_name",
        "product_name", "original_price", "discount_price", "discount_rate",
        "ranking_tags",
    ]

    csv_path = str(out_dir / "ranking_rows.csv")
    json_path = str(out_dir / "ranking_rows.json")
    summary_path = str(out_dir / "summary.json")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        f.write(to_csv(rows, csv_headers))

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    summary = {
        "runId": run_id,
        "startedAt": now_iso(),
        "finishedAt": now_iso(),
        "targetUrl": args.url,
        "categoriesRequested": args.category_limit,
        "categoriesCaptured": len(category_summaries),
        "rows": len(rows),
        "categorySummaries": category_summaries,
        "files": {"csvPath": csv_path, "jsonPath": json_path},
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

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
