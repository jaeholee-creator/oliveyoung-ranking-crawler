#!/usr/bin/env python3
"""올리브영 랭킹 크롤러 - camoufox (Firefox + BrowserForge) 기반.

CF 우회 안정화:
- camoufox: Firefox 기반 anti-detect 브라우저 (BrowserForge 핑거프린트)
- persistent_context + geoip + humanize
- CF 챌린지 감지 시 대기 후 재시도 (최대 3회)
- 리뷰 통계 API: curl_cffi HTTP 클라이언트 (TLS 핑거프린트 위장)

Fallback: camoufox 미설치 시 patchright 사용

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

# 브라우저 엔진 선택
try:
    from camoufox.sync_api import Camoufox
    USE_CAMOUFOX = True
except ImportError:
    USE_CAMOUFOX = False
    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="올리브영 랭킹 크롤러 (camoufox/patchright)")
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


def fetch_review_stats_http(goods_nos: list[str], batch_size: int = 100) -> dict[str, dict]:
    """curl_cffi로 리뷰 API 호출 — 배치별 세션 교체로 CF rate limit 우회.

    100건마다 세션을 닫고 새로 생성하여 CF가 동일 세션의 대량 요청으로
    인식하지 못하게 함. 배치 간 3~5초 쿨다운으로 자연스러운 패턴 유지.
    """
    from curl_cffi import requests as cffi_requests

    BROWSERS = ["chrome", "chrome120", "safari"]
    total = len(goods_nos)
    results: dict[str, dict] = {}
    fails = 0

    # 배치 분할
    batches = [goods_nos[i:i + batch_size] for i in range(0, total, batch_size)]
    print(f"[INFO] 리뷰 API: {total}건 → {len(batches)}개 배치 (배치당 {batch_size}건)")

    for batch_idx, batch in enumerate(batches):
        browser = BROWSERS[batch_idx % len(BROWSERS)]
        session = cffi_requests.Session(impersonate=browser)
        batch_ok = 0
        batch_fail = 0

        for gno in batch:
            url = f"https://m.oliveyoung.co.kr/review/api/v2/reviews/{gno}/stats"
            try:
                resp = session.get(url, timeout=10)
                data = resp.json()
                if data.get("status") == "SUCCESS":
                    d = data["data"]
                    results[gno] = {
                        "review_count": d.get("reviewCount"),
                        "rating": d.get("ratingDistribution", {}).get("averageRating"),
                    }
                    batch_ok += 1
                else:
                    batch_fail += 1
            except Exception:
                batch_fail += 1
            time.sleep(random.uniform(0.05, 0.15))

        fails += batch_fail
        session.close()

        print(f"[INFO] 배치 {batch_idx + 1}/{len(batches)} 완료: {batch_ok}건 성공, {batch_fail}건 실패 ({browser})")

        # 배치 실패율 체크: 첫 배치에서 80% 이상 실패 시 즉시 중단
        if batch_idx == 0 and batch_fail > len(batch) * 0.8:
            print(f"[ERROR] 첫 배치 실패율 과다 ({batch_fail}/{len(batch)}). 수집 중단.")
            break

        # 배치 간 쿨다운 (마지막 배치 제외)
        if batch_idx < len(batches) - 1:
            cooldown = random.uniform(3.0, 5.0)
            time.sleep(cooldown)

    # 실패 건 재시도 (1회)
    failed_nos = [gno for gno in goods_nos if gno not in results]
    if failed_nos:
        print(f"[INFO] 리뷰 재시도: {len(failed_nos)}건 → 5초 후 시작")
        time.sleep(5.0)
        retry_browser = "safari"
        retry_session = cffi_requests.Session(impersonate=retry_browser)
        retry_ok = 0
        for gno in failed_nos:
            url = f"https://m.oliveyoung.co.kr/review/api/v2/reviews/{gno}/stats"
            try:
                resp = retry_session.get(url, timeout=10)
                data = resp.json()
                if data.get("status") == "SUCCESS":
                    d = data["data"]
                    results[gno] = {
                        "review_count": d.get("reviewCount"),
                        "rating": d.get("ratingDistribution", {}).get("averageRating"),
                    }
                    retry_ok += 1
                    fails -= 1
            except Exception:
                pass
            time.sleep(random.uniform(0.05, 0.15))
        retry_session.close()
        print(f"[INFO] 재시도 결과: {retry_ok}/{len(failed_nos)} 추가 성공")

    print(f"[INFO] 리뷰 API 최종: {len(results)}/{total} 성공 ({total - len(results)}건 실패)")
    return results


def _crawl_with_camoufox(args: argparse.Namespace, user_data_dir: Path):
    """camoufox 브라우저 context manager를 반환."""
    import platform
    if args.headed:
        headless_mode = False
    elif platform.system() == "Linux":
        headless_mode = "virtual"  # Xvfb 자동 관리 (Linux 서버)
    else:
        headless_mode = True  # macOS/Windows: 네이티브 headless
    return Camoufox(
        persistent_context=True,
        headless=headless_mode,
        user_data_dir=str(user_data_dir),
        locale="ko-KR",
        humanize=True,
        enable_cache=True,
    )


def _crawl_with_patchright(args: argparse.Namespace, user_data_dir: Path):
    """patchright 폴백용 context manager 래퍼."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        from patchright.sync_api import sync_playwright
        with sync_playwright() as pw:
            import platform
            launch_kwargs = {
                "user_data_dir": str(user_data_dir),
                "headless": False,
                "no_viewport": True,
                "locale": "ko-KR",
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1920,1080",
                ],
            }
            if platform.machine() not in ("aarch64", "arm64"):
                launch_kwargs["channel"] = "chrome"
            context = pw.chromium.launch_persistent_context(**launch_kwargs)
            try:
                yield context
            finally:
                context.close()

    return _ctx()


def run() -> None:
    args = parse_args()

    if not USE_CAMOUFOX:
        try:
            from patchright.sync_api import sync_playwright  # noqa: F401
            print("[INFO] camoufox 미설치, patchright 폴백 사용")
        except ImportError:
            print("ERROR: camoufox / patchright 모두 미설치.", file=sys.stderr)
            print("  pip install camoufox  또는  pip install patchright", file=sys.stderr)
            sys.exit(1)
    else:
        print("[INFO] 브라우저 엔진: camoufox (Firefox + BrowserForge)")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z"
    out_dir = Path(os.getcwd()) / args.out_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # persistent context용 user_data_dir (브라우저 엔진별 분리)
    profile_name = ".browser_profile_camoufox" if USE_CAMOUFOX else ".browser_profile"
    user_data_dir = Path(os.getcwd()) / profile_name
    user_data_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] 실행 ID: {run_id}")
    print(f"[INFO] 출력 디렉토리: {out_dir}")
    print(f"[INFO] 대상 URL: {args.url}")
    print(f"[INFO] 카테고리 제한: {args.category_limit}")
    print(f"[INFO] 최소 수집 건수: {args.min_rows}")

    rows: list[dict] = []
    category_summaries: list[dict] = []

    # 브라우저 엔진 선택
    if USE_CAMOUFOX:
        browser_ctx = _crawl_with_camoufox(args, user_data_dir)
    else:
        browser_ctx = _crawl_with_patchright(args, user_data_dir)

    with browser_ctx as context:
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

        consecutive_fails = 0
        for idx, category in enumerate(target_categories):
            cat_label = f"[{idx + 1}/{len(target_categories)}] {category['name']} ({category['code']})"
            print(f"[INFO] 카테고리 처리 중: {cat_label}")

            button_code = category.get("rawCode", "")

            # 연속 2회 이상 실패 시 선제적 페이지 재로드
            if consecutive_fails >= 2:
                print(f"[WARN] {consecutive_fails}회 연속 실패 감지 → 페이지 재로드 후 재시도")
                page.goto(args.url, wait_until="domcontentloaded", timeout=120000)
                time.sleep(random.uniform(3.0, 5.0))
                consecutive_fails = 0

            # 버튼 대기 + 클릭 (실패 시 1회 재시도)
            clicked = False
            for attempt in range(2):
                locator = page.locator(f'button[data-ref-dispcatno="{button_code}"]').first
                try:
                    locator.wait_for(timeout=15000)
                except Exception:
                    if attempt == 0:
                        print(f"[WARN] 카테고리 버튼 대기 실패, 2초 후 재시도: {cat_label}")
                        time.sleep(2.0)
                        continue
                    print(f"[WARN] 카테고리 버튼 대기 최종 실패, 스킵: {cat_label}")
                    break

                # 클릭 전 스크롤 + 랜덤 딜레이
                try:
                    locator.scroll_into_view_if_needed(timeout=5000)
                except Exception:
                    pass
                time.sleep(random.uniform(0.3, 0.8))

                try:
                    locator.click(timeout=15000)
                    clicked = True
                    break
                except Exception:
                    if attempt == 0:
                        print(f"[WARN] 카테고리 클릭 실패, 2초 후 재시도: {cat_label}")
                        time.sleep(2.0)
                    else:
                        print(f"[WARN] 카테고리 클릭 최종 실패, 스킵: {cat_label}")

            if not clicked:
                consecutive_fails += 1
                category_summaries.append({
                    "categoryCode": category["code"],
                    "categoryName": category["name"],
                    "challengeDetected": False,
                    "goodsCount": 0,
                    "rankMin": None,
                    "rankMax": None,
                    "urlAfterClick": None,
                })
                continue

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
            consecutive_fails = 0 if len(normalized) > 0 else consecutive_fails + 1

            # 카테고리 간 랜덤 딜레이
            if idx < len(target_categories) - 1:
                time.sleep(random.uniform(0.5, 1.5))

        page.close()

    # 리뷰 통계 API 호출 (curl_cffi HTTP, 브라우저 세션 불필요)
    unique_goods_nos = list({r["goods_no"] for r in rows if r.get("goods_no")})
    print(f"[INFO] 리뷰 통계 조회: {len(unique_goods_nos)}개 상품")

    review_total = len(unique_goods_nos)
    review_stats = fetch_review_stats_http(unique_goods_nos)
    review_success = len(review_stats)
    review_fail = review_total - review_success
    print(f"[INFO] 리뷰 통계 수집 완료: {review_success}/{review_total}개 (실패 {review_fail}건)")

    # rows에 리뷰 데이터 병합
    for row in rows:
        gno = row.get("goods_no")
        stats = review_stats.get(gno, {})
        row["review_count"] = stats.get("review_count")
        row["rating"] = stats.get("rating")

    review_summary = {
        "total": review_total,
        "success": review_success,
        "fail": review_fail,
        "rate": round(review_success / review_total * 100, 1) if review_total else 0,
    }
    _write_output(out_dir, rows, category_summaries, args, run_id, review_summary)

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
    review_summary: dict | None = None,
) -> None:
    """파일 출력 (성공/실패 모두 기록)."""
    csv_headers = [
        "collected_at_utc", "category_code", "category_name", "rank",
        "goods_no", "detail_disp_cat_no", "detail_url", "brand_name",
        "product_name", "original_price", "discount_price", "discount_rate",
        "ranking_tags",
        "review_count", "rating",
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
        "reviewStats": review_summary,
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
