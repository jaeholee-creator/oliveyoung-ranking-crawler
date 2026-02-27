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
import threading
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlsplit, urlunsplit

MAX_CF_RETRIES = 3
CF_WAIT_SECONDS = 15
REVIEW_API = "https://m.oliveyoung.co.kr/review/api/v2/reviews/{goods_no}/stats"
BROWSERS = ["chrome", "chrome120", "safari"]
DEFAULT_CATEGORY_LIMIT = 21
DEFAULT_MIN_ROWS = 2100
DEFAULT_CATEGORY_ROWS = 100
DEFAULT_CATEGORY_RETRIES = 2
DEFAULT_PAGE_TIMEOUT_MS = 45000
DEFAULT_CATEGORY_SCROLL_ATTEMPTS = 3
DEFAULT_CATEGORY_SCROLL_SLEEP_SEC = (0.35, 0.85)

REVIEW_WORKERS = 18
REVIEW_RETRIES = 3
REVIEW_TIMEOUT = 10.0
REVIEW_BACKOFF_SECONDS = (1.0, 2.5, 6.0, 12.0)
REVIEW_WORKER_HARD_CAP = 24
REVIEW_CACHE_FILE = Path("cache/review_cache.json")
REVIEW_CACHE_TTL_HOURS = 8.0
REVIEW_CACHE_MAX_ENTRIES = 30000


DEFAULT_CATEGORIES = [
    {"name": "전체", "rawCode": "", "code": "ALL", "disp_cat_no": "900000100100001", "flt_cat_no": ""},
    {"name": "스킨케어", "rawCode": "10000010001", "code": "10000010001", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010001"},
    {"name": "마스크팩", "rawCode": "10000010009", "code": "10000010009", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010009"},
    {"name": "클렌징", "rawCode": "10000010010", "code": "10000010010", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010010"},
    {"name": "선케어", "rawCode": "10000010011", "code": "10000010011", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010011"},
    {"name": "메이크업", "rawCode": "10000010002", "code": "10000010002", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010002"},
    {"name": "네일", "rawCode": "10000010012", "code": "10000010012", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010012"},
    {"name": "메이크업 툴", "rawCode": "10000010006", "code": "10000010006", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010006"},
    {"name": "더모 코스메틱", "rawCode": "10000010008", "code": "10000010008", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010008"},
    {"name": "맨즈케어", "rawCode": "10000010007", "code": "10000010007", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010007"},
    {"name": "향수/디퓨저", "rawCode": "10000010005", "code": "10000010005", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010005"},
    {"name": "헤어케어", "rawCode": "10000010004", "code": "10000010004", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010004"},
    {"name": "바디케어", "rawCode": "10000010003", "code": "10000010003", "disp_cat_no": "900000100100001", "flt_cat_no": "10000010003"},
    {"name": "건강식품", "rawCode": "10000020001", "code": "10000020001", "disp_cat_no": "900000100100001", "flt_cat_no": "10000020001"},
    {"name": "푸드", "rawCode": "10000020002", "code": "10000020002", "disp_cat_no": "900000100100001", "flt_cat_no": "10000020002"},
    {"name": "구강용품", "rawCode": "10000020003", "code": "10000020003", "disp_cat_no": "900000100100001", "flt_cat_no": "10000020003"},
    {"name": "헬스/건강용품", "rawCode": "10000020005", "code": "10000020005", "disp_cat_no": "900000100100001", "flt_cat_no": "10000020005"},
    {"name": "위생용품", "rawCode": "10000020004", "code": "10000020004", "disp_cat_no": "900000100100001", "flt_cat_no": "10000020004"},
    {"name": "패션", "rawCode": "10000030007", "code": "10000030007", "disp_cat_no": "900000100100001", "flt_cat_no": "10000030007"},
    {"name": "홈리빙/가전", "rawCode": "10000030005", "code": "10000030005", "disp_cat_no": "900000100100001", "flt_cat_no": "10000030005"},
    {"name": "취미/팬시", "rawCode": "10000030006", "code": "10000030006", "disp_cat_no": "900000100100001", "flt_cat_no": "10000030006"},
]

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
    parser.add_argument("--category-limit", type=int, default=DEFAULT_CATEGORY_LIMIT)
    parser.add_argument("--category-wait-ms", type=int, default=2500)
    parser.add_argument("--headed", action="store_true", default=False)
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS, help="최소 수집 건수 (미달 시 exit 1)")
    parser.add_argument("--review-workers", type=int, default=REVIEW_WORKERS, help="리뷰 API 동시 요청 수")
    parser.add_argument("--review-retries", type=int, default=REVIEW_RETRIES, help="리뷰 API 재시도 횟수")
    parser.add_argument("--review-timeout", type=float, default=REVIEW_TIMEOUT, help="리뷰 API 타임아웃(초)")
    parser.add_argument("--category-sort-by-page", action="store_true", default=False, help="추출된 버튼 순서 대신 기본 카테고리 순서 사용")
    parser.add_argument(
        "--category-rows",
        type=int,
        default=DEFAULT_CATEGORY_ROWS,
        help="카테고리별 수집 목표 건수(기본 100)",
    )
    parser.add_argument("--category-retries", type=int, default=DEFAULT_CATEGORY_RETRIES, help="카테고리 진입 실패 시 재시도 횟수")
    parser.add_argument("--page-timeout-ms", type=int, default=DEFAULT_PAGE_TIMEOUT_MS, help="브라우저 페이지 로드 타임아웃(밀리초)")
    parser.add_argument(
        "--category-scroll-attempts",
        type=int,
        default=DEFAULT_CATEGORY_SCROLL_ATTEMPTS,
        help="카테고리별 상품 미노출 시 추가 스크롤 시도 횟수",
    )
    parser.add_argument(
        "--review-cache-path",
        default=str(REVIEW_CACHE_FILE),
        help="리뷰 통계 캐시 파일 경로 (빈 값이면 캐시 비활성화)",
    )
    parser.add_argument(
        "--review-cache-ttl-hours",
        type=float,
        default=REVIEW_CACHE_TTL_HOURS,
        help="리뷰 캐시 TTL(시간), 0 이하면 캐시 미사용",
    )
    parser.add_argument(
        "--review-cache-max-entries",
        type=int,
        default=REVIEW_CACHE_MAX_ENTRIES,
        help="캐시 최대 저장 건수",
    )
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


def load_review_cache(cache_path: Path, ttl_hours: float, max_entries: int) -> dict[str, dict[str, Any]]:
    """상품 리뷰 캐시 로드. 만료/깨진 데이터는 무시하고 최신 데이터만 남긴다."""
    if ttl_hours <= 0:
        return {}
    if not cache_path.exists():
        return {}

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(raw, dict):
        return {}

    now_ts = time.time()
    cutoff = now_ts - (ttl_hours * 3600)
    cache: dict[str, dict[str, Any]] = {}
    for goods_no, payload in raw.items():
        if not isinstance(goods_no, str) or not goods_no:
            continue
        if not isinstance(payload, dict):
            continue
        cached_at = payload.get("cachedAt")
        if cached_at is None:
            continue
        try:
            if float(cached_at) < cutoff:
                continue
        except (TypeError, ValueError):
            continue

        review_count = payload.get("review_count")
        rating = payload.get("rating")
        if review_count is None and rating is None:
            continue
        cache[goods_no] = {
            "review_count": review_count,
            "rating": rating,
            "cachedAt": float(cached_at),
        }

    if max_entries > 0 and len(cache) > max_entries:
        sorted_items = sorted(cache.items(), key=lambda it: float(it[1].get("cachedAt") or 0), reverse=True)
        cache = dict(sorted_items[:max_entries])

    return cache


def save_review_cache(cache: dict[str, dict[str, Any]], cache_path: Path, max_entries: int) -> None:
    """상품 리뷰 캐시를 파일로 저장."""
    if max_entries > 0 and len(cache) > max_entries:
        items = sorted(cache.items(), key=lambda it: float(it[1].get("cachedAt") or 0), reverse=True)
        cache = dict(items[:max_entries])

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


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


def normalize_categories(
    extracted: list[dict],
    limit: int,
    sort_by_default: bool,
) -> list[dict]:
    """사이트에서 추출한 카테고리와 기본 카테고리 목록을 정렬/보정."""
    by_code = {}
    for cat in DEFAULT_CATEGORIES:
        by_code[str(cat["rawCode"] or "").strip()] = cat
        if cat.get("code") == "ALL":
            by_code["ALL"] = cat

    ordered_codes = []
    if sort_by_default or not extracted:
        ordered_codes = [cat["code"] for cat in DEFAULT_CATEGORIES]
    else:
        for item in extracted:
            code = str(item.get("code") or item.get("rawCode") or "").strip()
            if code == "":
                code = "ALL"
            if code in by_code:
                ordered_codes.append(code)

        # 추출된 목록이 기본 목록과 달라도 전체 카테고리 수를 맞추기 위해 기본 순서를 보완
        for cat in DEFAULT_CATEGORIES:
            if cat["code"] not in ordered_codes:
                ordered_codes.append(cat["code"])

    # 중복 제거 후 limit 적용
    dedup = []
    seen = set()
    for code in ordered_codes:
        if code in seen:
            continue
        seen.add(code)
        dedup.append(by_code.get(code))
    if limit <= 0:
        return [x for x in dedup if x]
    return [x for x in dedup if x][:limit]


def build_ranking_url(base_url: str, category: dict) -> str:
    parts = urlsplit(base_url)
    params = parse_qs(parts.query, keep_blank_values=True)

    for key in ("dispCatNo", "fltDispCatNo", "dispCatNoList", "fltCatNo"):
        params.pop(key, None)

    disp_cat_no = (category.get("disp_cat_no") or "").strip()
    if not disp_cat_no and category.get("rawCode"):
        disp_cat_no = ""

    if disp_cat_no:
        params["dispCatNo"] = [disp_cat_no]
    flt_cat_no = (category.get("flt_cat_no") or "").strip()
    if flt_cat_no:
        params["fltDispCatNo"] = [flt_cat_no]

    query = urlencode(params, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def extract_items(page: Any) -> list[dict]:
    return page.locator("body").evaluate_all(
        """() => {
            const out = [];
            const seen = new Set();

            const pickText = (sel, root) => {
                const node = root.querySelector(sel);
                return node && node.textContent ? node.textContent.trim() : '';
            };

            const pickGoodsNo = (href, root) => {
                if (href) {
                    try {
                        const parsed = new URL(href, 'https://www.oliveyoung.co.kr');
                        const goodsNo = parsed.searchParams.get('goodsNo');
                        if (goodsNo) return goodsNo;
                    } catch (_) {}
                }
                const ref = root && root.getAttribute ? root.getAttribute('data-ref-goodsno') : null;
                if (ref) return ref;
                return null;
            };

            const addCard = (node, idx) => {
                const thumb = node.querySelector('a.prd_thumb[href*=\"getGoodsDetail.do?goodsNo=\"]')
                    || node.querySelector('a[data-ref-goodsno]');
                if (!thumb && !node.getAttribute('data-ref-goodsno')) return;

                const href = thumb?.getAttribute('href') || '';
                const goodsNo = pickGoodsNo(href, thumb || node);
                if (!goodsNo || seen.has(goodsNo)) return;
                seen.add(goodsNo);

                let rank = null;
                const rankRaw = pickText('.thumb_flag', node) || pickText('[class*=\"rank\"]', node);
                if (rankRaw && /^\\d+$/.test(rankRaw)) rank = Number(rankRaw);
                if (rank == null) rank = idx + 1;

                let dispCatNo = null;
                try {
                    if (href) {
                        const parsed = new URL(href, 'https://www.oliveyoung.co.kr');
                        dispCatNo = parsed.searchParams.get('dispCatNo');
                    }
                } catch (_) {}

                const originalPriceText = pickText('.prd_price .tx_org .tx_num', node)
                    || pickText('[class*=\"o_price\"]', node)
                    || '';
                const discountPriceText = pickText('.prd_price .tx_cur .tx_num', node)
                    || pickText('[class*=\"sale_price\"]', node)
                    || '';
                const tags = Array.from(node.querySelectorAll('.prd_flag .icon_flag, .tx_badge, [class*=\"badge\"]'))
                    .map((node) => (node.textContent || '').trim())
                    .filter(Boolean);

                out.push({
                    href,
                    goodsNo,
                    dispCatNo,
                    rank,
                    brand: pickText('.tx_brand', node),
                    productName: pickText('.tx_name', node),
                    originalPriceText,
                    discountPriceText,
                    tags,
                });
            };

            const cards = Array.from(document.querySelectorAll('div.prd_info, ul.cate_prd_list li'));
            cards.forEach((node, idx) => addCard(node, idx));
            return out;
        }"""
    )


def wait_for_ranking_items(page: Any, timeout_ms: int = 12000, min_count: int = 1) -> int:
    """카테고리 페이지 상품 노출을 기다리고 실제 추출 개수를 반환."""
    try:
        page.wait_for_function(
            """(minCount) => {
                const nodes = document.querySelectorAll('div.prd_info, ul.cate_prd_list li');
                return nodes && nodes.length >= minCount;
            }""",
            min_count,
            timeout=timeout_ms,
        )
    except Exception:
        pass
    try:
        return page.locator("div.prd_info, ul.cate_prd_list li").count()
    except Exception:
        return 0


def ensure_category_page_ready(page: Any, category: dict, target_url: str, args: argparse.Namespace) -> bool:
    """카테고리 URL 진입 후 상품 목록이 존재할 때 True."""
    required_items = max(1, min(args.category_rows, 100)) if args.category_rows > 0 else 1
    min_items_for_ready = max(20, min(required_items, 80))

    for _ in range(max(1, args.category_retries)):
        # 1차: URL 직접 진입
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=args.page_timeout_ms)
            time.sleep(random.uniform(0.8, 1.5))
            if detect_challenge(page):
                if not wait_for_cf_resolution(page, CF_WAIT_SECONDS):
                    continue
            simulate_human(page)
            count = wait_for_ranking_items(page, args.category_wait_ms, min_count=min_items_for_ready)
            if count > 0:
                return True
        except Exception:
            pass

        # 2차: 버튼 클릭 fallback
        button_code = category.get("rawCode", "")
        try:
            locator = page.locator(f'button[data-ref-dispcatno="{button_code}"]').first
            locator.wait_for(timeout=8000)
            locator.scroll_into_view_if_needed(timeout=3000)
            locator.click(timeout=8000)
            time.sleep(random.uniform(0.4, 1.0))
            if detect_challenge(page):
                if not wait_for_cf_resolution(page, CF_WAIT_SECONDS):
                    continue
            count = wait_for_ranking_items(page, args.category_wait_ms, min_count=min_items_for_ready)
            if count > 0:
                return True
        except Exception:
            continue
    return False


def collect_category_items(page: Any, target_rows: int, scroll_attempts: int) -> list[dict]:
    """카테고리에서 상품 카드 추출 + 필요 시 스크롤로 추가 로딩."""
    items = extract_items(page)
    if target_rows <= 0 or len(items) >= target_rows:
        return items

    stable_count = 0
    previous_count = len(items)
    for _ in range(max(1, scroll_attempts)):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        except Exception:
            pass
        time.sleep(random.uniform(*DEFAULT_CATEGORY_SCROLL_SLEEP_SEC))
        now_items = extract_items(page)
        if len(now_items) >= target_rows:
            return now_items
        if len(now_items) == previous_count:
            stable_count += 1
            if stable_count >= 2:
                break
        else:
            stable_count = 0
            previous_count = len(now_items)
        items = now_items

    return items


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


def _pick_browser(user_no: int) -> str:
    return BROWSERS[user_no % len(BROWSERS)]


_review_threads = threading.local()


def _get_review_session(cffi_requests: Any, browser: str):
    """Thread-local cffi session 캐시로 동일 스레드 재요청 비용을 줄인다."""
    sessions = getattr(_review_threads, "sessions", None)
    if sessions is None:
        sessions = {}
        _review_threads.sessions = sessions
    if browser not in sessions:
        sessions[browser] = cffi_requests.Session(impersonate=browser)
    return sessions[browser]


def _parse_review_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("status") != "SUCCESS":
        return {}
    data = payload.get("data") or {}
    return {
        "review_count": data.get("reviewCount"),
        "rating": (data.get("ratingDistribution") or {}).get("averageRating"),
    }


def _review_backoff(attempt: int) -> float:
    idx = min(max(attempt - 1, 0), len(REVIEW_BACKOFF_SECONDS) - 1)
    return REVIEW_BACKOFF_SECONDS[idx] + random.uniform(0.2, 0.8)


def fetch_review_stats_http(
    goods_nos: list[str],
    *,
    max_workers: int = REVIEW_WORKERS,
    timeout: float = REVIEW_TIMEOUT,
    retries: int = REVIEW_RETRIES,
) -> dict[str, dict]:
    """curl_cffi로 리뷰 API 호출 — 병렬 + 재시도."""
    from curl_cffi import requests as cffi_requests

    unique_nos = list(dict.fromkeys([x for x in goods_nos if x]))
    if not unique_nos:
        return {}

    print(
        f"[INFO] 리뷰 API 병렬 수집: {len(unique_nos)}건 "
        f"(workers={max_workers}, timeout={timeout}, retries={retries})"
    )

    def _fetch_one(gno: str, worker_id: int) -> tuple[str, dict[str, Any] | None]:
        browser = _pick_browser(worker_id)
        url = REVIEW_API.format(goods_no=gno)
        headers = {"Accept": "application/json, text/plain, */*"}
        session = _get_review_session(cffi_requests, browser)

        for attempt in range(1, retries + 1):
            try:
                resp = session.get(url, headers=headers, timeout=timeout)
                status = int(resp.status_code)
                if status == 200:
                    parsed = _parse_review_payload(resp.json())
                    if parsed:
                        return gno, parsed
                if status in (403, 429, 500, 502, 503):
                    time.sleep(_review_backoff(attempt))
                    continue
                return gno, None
            except Exception:
                time.sleep(_review_backoff(attempt))

        return gno, None

    def _run_batch(
        targets: list[str],
        workers: int,
        base_worker_id: int,
    ) -> tuple[list[str], int, dict[str, dict[str, Any]]]:
        ok = 0
        failed: list[str] = []
        batch_results: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_fetch_one, gno, base_worker_id + idx): gno
                for idx, gno in enumerate(targets)
            }
            for future in as_completed(futures):
                gno = futures[future]
                try:
                    key, payload = future.result()
                except Exception:
                    key = gno
                    payload = None
                if payload:
                    ok += 1
                    batch_results[key] = payload
                else:
                    failed.append(key)
        return failed, ok, batch_results

    # 1차: 전체 대상 병렬 수집
    results: dict[str, dict] = {}
    pending = unique_nos
    worker_count = max(1, min(max_workers, REVIEW_WORKER_HARD_CAP, len(pending)))
    failed, ok_count, batch_results = _run_batch(pending, worker_count, 0)
    results.update(batch_results)
    print(f"[INFO] 리뷰 1차 완료: 성공 {ok_count}건 / {len(unique_nos)}건")

    # 2차: 실패 항목만 소규모 재시도
    if failed:
        retry_workers = max(
            1,
            min(max(4, len(failed) // 20), len(failed), REVIEW_WORKER_HARD_CAP),
        )
        failed2, ok2, retry_results = _run_batch(failed, retry_workers, len(pending))
        if retry_results:
            results.update(retry_results)
        failed = failed2
        print(f"[INFO] 리뷰 재시도 완료: 추가 성공 {ok2}건 / 잔여 {len(failed)}건")

    if failed:
        # 최종 미수집은 빈 dict로 남겨 데이터 무결성 유지(행은 유지)
        for gno in failed:
            results.setdefault(gno, {})

    final_ok = len([v for v in results.values() if v])
    print(f"[INFO] 리뷰 API 최종: {final_ok}/{len(unique_nos)} 수집 성공 ({len(unique_nos) - final_ok}건 실패)")
    return {gno: results.get(gno, {}) for gno in unique_nos}


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
    review_cache: dict[str, dict[str, Any]] = {}
    review_cache_path: Path | None = None
    review_cache_path_value = (args.review_cache_path or "").strip()
    if review_cache_path_value and args.review_cache_ttl_hours > 0:
        review_cache_path = Path(review_cache_path_value).expanduser()
        review_cache = load_review_cache(
            review_cache_path,
            ttl_hours=args.review_cache_ttl_hours,
            max_entries=args.review_cache_max_entries,
        )
        print(f"[INFO] 리뷰 캐시 로드: {len(review_cache)}건")

    # 브라우저 엔진 선택
    if USE_CAMOUFOX:
        browser_ctx = _crawl_with_camoufox(args, user_data_dir)
    else:
        browser_ctx = _crawl_with_patchright(args, user_data_dir)

    try:
        with browser_ctx as context:
            page = context.new_page()

            # 초기 페이지 로드 + CF 챌린지 대기/재시도
            loaded = False
            for attempt in range(1, MAX_CF_RETRIES + 1):
                print(f"[INFO] 페이지 로딩 시도 {attempt}/{MAX_CF_RETRIES}: {args.url}")
                page.goto(args.url, wait_until="domcontentloaded", timeout=args.page_timeout_ms)
                time.sleep(random.uniform(1.2, 2.0))

                # 마우스 움직임으로 human-like
                simulate_human(page)

                if detect_challenge(page):
                    resolved = wait_for_cf_resolution(page, CF_WAIT_SECONDS)
                    if resolved:
                        loaded = True
                        break
                    # 재시도: 페이지 새로고침
                    print(f"[WARN] CF 챌린지 미해결, 재시도...")
                    page.reload(wait_until="domcontentloaded", timeout=args.page_timeout_ms)
                    time.sleep(random.uniform(1.8, 3.0))
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

            # 카테고리 목록 추출 + 정렬/보정
            extracted_categories = extract_categories(page)
            target_categories = normalize_categories(
                extracted_categories,
                limit=args.category_limit,
                sort_by_default=args.category_sort_by_page,
            )
            print(
                f"[INFO] 발견된 카테고리: {len(extracted_categories)}개, "
                f"수집 대상: {len(target_categories)}개"
            )

            if not target_categories:
                print("[ERROR] 카테고리를 찾을 수 없음")
                _write_output(out_dir, rows, category_summaries, args, run_id)
                sys.exit(1)

            consecutive_fails = 0
            for idx, category in enumerate(target_categories):
                cat_label = f"[{idx + 1}/{len(target_categories)}] {category['name']} ({category['code']})"
                print(f"[INFO] 카테고리 처리 중: {cat_label}")
                target_url = build_ranking_url(args.url, category)

                if consecutive_fails >= 2:
                    print(f"[WARN] {consecutive_fails}회 연속 실패 감지 → 페이지 재로드 후 재시도")
                    try:
                        page.goto(args.url, wait_until="domcontentloaded", timeout=args.page_timeout_ms)
                        time.sleep(random.uniform(0.8, 1.5))
                        consecutive_fails = 0
                    except Exception as reload_err:
                        print(f"[WARN] 페이지 재로드 실패 → 카테고리 수집 중단: {reload_err}")
                        break

                ready = ensure_category_page_ready(page, category, target_url, args)
                if not ready:
                    consecutive_fails += 1
                    category_summaries.append({
                        "categoryCode": category["code"],
                        "categoryName": category["name"],
                        "challengeDetected": False,
                        "goodsCount": 0,
                        "rankMin": None,
                        "rankMax": None,
                        "urlAfterClick": target_url,
                    })
                    continue

                # 상품 데이터 추출
                items = extract_items(page)
                if not items:
                    time.sleep(random.uniform(0.4, 0.9))
                    items = collect_category_items(page, args.category_rows, args.category_scroll_attempts)
                elif args.category_rows > 0 and len(items) < args.category_rows:
                    items = collect_category_items(page, args.category_rows, args.category_scroll_attempts)

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
                if args.category_rows > 0:
                    normalized = normalized[: args.category_rows]

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
                    time.sleep(random.uniform(0.3, 0.8))

            # 0건 카테고리 재시도 (페이지 재로드 후 1회)
            zero_cats = [
                c for c in extracted_categories
                if any(
                    cs["categoryCode"] == c["code"] and cs.get("goodsCount") == 0
                    for cs in category_summaries
                )
            ]
            if zero_cats:
                print(f"[INFO] 0건 카테고리 재시도: {len(zero_cats)}개 → 페이지 재로드 후 시작")
                try:
                    page.goto(args.url, wait_until="domcontentloaded", timeout=args.page_timeout_ms)
                    time.sleep(random.uniform(3.0, 5.0))
                    for retry_cat in zero_cats:
                        cat_label_r = f"[재시도] {retry_cat['name']} ({retry_cat['code']})"
                        print(f"[INFO] 카테고리 재시도: {cat_label_r}")
                        target_url_r = build_ranking_url(args.url, retry_cat)
                        ready_r = ensure_category_page_ready(page, retry_cat, target_url_r, args)
                        if not ready_r:
                            print(f"[WARN] 재시도 실패: {cat_label_r}")
                            time.sleep(random.uniform(0.5, 1.5))
                            continue
                        items_r = extract_items(page)
                        if not items_r:
                            time.sleep(random.uniform(1.0, 2.0))
                            items_r = extract_items(page)
                        dedup_r: dict[str, dict] = {}
                        for item in items_r:
                            gno = item.get("goodsNo")
                            if gno and gno not in dedup_r:
                                dedup_r[gno] = item
                        normalized_r = sorted(
                            dedup_r.values(),
                            key=lambda x: x.get("rank") if isinstance(x.get("rank"), (int, float)) else 9999,
                        )
                        if args.category_rows > 0:
                            normalized_r = normalized_r[: args.category_rows]
                        for item in normalized_r:
                            orig, disc, rate = normalize_prices(
                                item.get("originalPriceText", ""),
                                item.get("discountPriceText", ""),
                                item.get("tags", []),
                            )
                            rows.append({
                                "collected_at_utc": now_iso(),
                                "category_code": retry_cat["code"],
                                "category_name": retry_cat["name"],
                                "rank": item.get("rank"),
                                "goods_no": item.get("goodsNo"),
                                "detail_disp_cat_no": item.get("dispCatNo"),
                                "detail_url": build_detail_url(item.get("href", "")),
                                "brand_name": item.get("brand", ""),
                                "product_name": item.get("productName", ""),
                                "original_price": orig,
                                "discount_price": disc,
                                "discount_rate": rate,
                                "ranking_tags": ",".join(item.get("tags", [])),
                            })
                        ranks_r = [
                            item.get("rank") for item in normalized_r
                            if isinstance(item.get("rank"), (int, float))
                        ]
                        for cs in category_summaries:
                            if cs["categoryCode"] == retry_cat["code"]:
                                cs["goodsCount"] = len(normalized_r)
                                cs["rankMin"] = min(ranks_r) if ranks_r else None
                                cs["rankMax"] = max(ranks_r) if ranks_r else None
                                cs["urlAfterClick"] = page.url
                                break
                        print(f"[INFO] 재시도 결과: {cat_label_r} → {len(normalized_r)}개 상품")
                        time.sleep(random.uniform(0.5, 1.5))
                except Exception as retry_err:
                    print(f"[WARN] 0건 카테고리 재시도 중 오류: {retry_err}")

            try:
                page.close()
            except Exception:
                pass

    except Exception:
        _write_output(out_dir, rows, category_summaries, args, run_id)
        raise

    # 리뷰 통계 API 호출 (curl_cffi HTTP, 브라우저 세션 불필요)
    unique_goods_nos = list({r["goods_no"] for r in rows if r.get("goods_no")})
    print(f"[INFO] 리뷰 통계 조회: {len(unique_goods_nos)}개 상품")

    review_total = len(unique_goods_nos)
    cached_review_stats = {
        goods_no: {
            "review_count": payload.get("review_count"),
            "rating": payload.get("rating"),
        }
        for goods_no, payload in review_cache.items()
        if goods_no in unique_goods_nos and isinstance(payload, dict)
    }
    missing_review_goods = [g for g in unique_goods_nos if g not in cached_review_stats]
    cache_hit_count = len(unique_goods_nos) - len(missing_review_goods)

    remote_review_stats = {}
    if missing_review_goods:
        remote_review_stats = fetch_review_stats_http(
            missing_review_goods,
            max_workers=args.review_workers,
            timeout=args.review_timeout,
            retries=args.review_retries,
        )

    if review_cache_path is not None:
        refreshed_cache = review_cache.copy()
        refresh_ts = time.time()
        for goods_no, payload in remote_review_stats.items():
            if not payload:
                continue
            refreshed_cache[goods_no] = {
                "review_count": payload.get("review_count"),
                "rating": payload.get("rating"),
                "cachedAt": refresh_ts,
            }
        try:
            save_review_cache(
                refreshed_cache,
                review_cache_path,
                max_entries=args.review_cache_max_entries,
            )
        except Exception as exc:
            print(f"[WARN] 리뷰 캐시 저장 실패: {exc}")

    review_stats = {}
    for goods_no in unique_goods_nos:
        review_stats[goods_no] = (
            remote_review_stats.get(goods_no)
            or cached_review_stats.get(goods_no, {})
            or {}
        )
    review_success = len([v for v in review_stats.values() if v])
    review_fail = review_total - review_success
    cache_hit_rate = round(cache_hit_count / review_total * 100, 1) if review_total else 0
    print(
        "[INFO] 리뷰 통계 수집 완료: "
        f"캐시 {cache_hit_count}건 ({cache_hit_rate}%), "
        f"신규 {len(missing_review_goods)}건, "
        f"성공 {review_success}/{review_total}개 (실패 {review_fail}건)"
    )

    # rows에 리뷰 데이터 병합
    for row in rows:
        gno = row.get("goods_no")
        stats = review_stats.get(gno, {})
        row["review_count"] = stats.get("review_count")
        row["rating"] = stats.get("rating")

    review_summary = {
        "total": review_total,
        "cacheHit": cache_hit_count,
        "cacheHitRate": cache_hit_rate,
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
