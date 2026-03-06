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
import socket
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
ALT_REVIEW_API = "https://product-review-service.oliveyoung.com/api/v1/domestic-reviews/count"
DETAIL_SHORT_URL = "https://www.oliveyoung.co.kr/store/G.do?goodsNo={goods_no}"
DESCRIPTION_API = "https://www.oliveyoung.co.kr/goods/api/v1/description?goodsNumber={goods_no}"
EXTRA_API = "https://www.oliveyoung.co.kr/goods/api/v1/extra"
QNA_API = "https://www.oliveyoung.co.kr/claim-front/api/v1/goods/getGoodsQnACount?goodsNo={goods_no}"
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
DETAIL_WORKERS = 6
DETAIL_RETRIES = 2
DETAIL_TIMEOUT = 15.0


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
    _SYNC_PLAYWRIGHT = None

try:
    from patchright.sync_api import sync_playwright as _SYNC_PLAYWRIGHT
except ImportError:
    try:
        from playwright.sync_api import sync_playwright as _SYNC_PLAYWRIGHT
    except ImportError:
        _SYNC_PLAYWRIGHT = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="올리브영 랭킹 크롤러 (camoufox/patchright)")
    parser.add_argument(
        "--browser-engine",
        choices=["auto", "camoufox", "playwright"],
        default="auto",
        help="브라우저 엔진 선택 (auto: 설치 순 우선순위)",
    )
    parser.add_argument("--url", default="https://www.oliveyoung.co.kr/store/main/getBestList.do")
    parser.add_argument("--out-dir", default="output/ranking_scrapling")
    parser.add_argument("--category-limit", type=int, default=DEFAULT_CATEGORY_LIMIT)
    parser.add_argument("--category-wait-ms", type=int, default=2500)
    parser.add_argument("--headed", action="store_true", default=False)
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS, help="최소 수집 건수 (미달 시 exit 1)")
    parser.add_argument("--review-workers", type=int, default=REVIEW_WORKERS, help="리뷰 API 동시 요청 수")
    parser.add_argument("--review-retries", type=int, default=REVIEW_RETRIES, help="리뷰 API 재시도 횟수")
    parser.add_argument("--review-timeout", type=float, default=REVIEW_TIMEOUT, help="리뷰 API 타임아웃(초)")
    parser.add_argument("--detail-workers", type=int, default=DETAIL_WORKERS, help="상세/extra API 동시 요청 수")
    parser.add_argument("--detail-retries", type=int, default=DETAIL_RETRIES, help="상세/extra API 재시도 횟수")
    parser.add_argument("--detail-timeout", type=float, default=DETAIL_TIMEOUT, help="상세/extra API 타임아웃(초)")
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="브라우저 실행 없이 목업 데이터로 결과만 출력",
    )
    parser.add_argument(
        "--dry-run-rows",
        type=int,
        default=0,
        help="dry-run 시 카테고리당 생성할 목업 행 수(0이면 생성 안함)",
    )
    parser.add_argument(
        "--review-only",
        action="store_true",
        default=False,
        help="카테고리 수집 없이 goods_no 기준으로 리뷰 API만 단독 테스트",
    )
    parser.add_argument(
        "--review-goods",
        default="",
        help="review-only 모드에서 테스트할 goods_no 목록(쉼표 구분)",
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
                const tags = Array.from(node.querySelectorAll('.prd_flag .icon_flag, .tx_badge, [class*=\"badge\"], [class*=\"Flag_flag\"]'))
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


def _extract_rsc(html: str) -> str:
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', html)
    result = ""
    for chunk in chunks:
        try:
            result += json.loads(f'"{chunk}"')
        except Exception:
            result += chunk
    return result


def _build_extra_api_body(goods_no: str, rsc: str) -> dict[str, Any] | None:
    def find(key: str) -> str | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', rsc)
        return m.group(1) if m else None

    goods_type = find("goodsTypeCode")
    supplier = find("supplier") or ""
    online_brand = find("onlineBrand") or ""
    if not goods_type or not supplier or not online_brand:
        return None

    lower_category_number = find("lowerCategoryNumber") or ""
    brand = find("brand") or ""
    options_raw = re.findall(
        r'"goodsNumber":"[^"]+","optionNumber":"([^"]+)","standardCode":"([^"]+)","optionName":"([^"]*)"',
        rsc,
    )
    options = [
        {
            "goodsNumber": goods_no,
            "optionNumber": option_number,
            "standardCode": standard_code,
            "optionName": option_name,
            "brand": brand,
            "lowerCategoryNumber": lower_category_number,
        }
        for option_number, standard_code, option_name in options_raw
    ]
    if not options:
        standard_code = find("standardCode")
        if standard_code:
            options = [
                {
                    "goodsNumber": goods_no,
                    "optionNumber": "001",
                    "standardCode": standard_code,
                    "optionName": "",
                    "brand": brand,
                    "lowerCategoryNumber": lower_category_number,
                }
            ]
    if not options:
        return None

    return {
        "goodsNumber": goods_no,
        "goodsTypeCode": goods_type,
        "goodsSectionCode": find("goodsSectionCode") or "10",
        "deliveryPolicyNumber": find("deliveryPolicyNumber") or "1",
        "tradeCode": find("tradeCode") or "1",
        "supplier": supplier,
        "onlineBrand": online_brand,
        "brand": brand,
        "options": options,
    }


def _find_meta_content(html: str, attr_name: str, attr_value: str) -> str | None:
    escaped = re.escape(attr_value)
    patterns = [
        rf'<meta[^>]+{attr_name}=["\']{escaped}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+{attr_name}=["\']{escaped}["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _rsc_find_string(rsc: str, key: str) -> str | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', rsc)
    return match.group(1) if match else None


def _rsc_find_bool(rsc: str, key: str) -> bool | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*(true|false)', rsc)
    if not match:
        return None
    return match.group(1) == "true"


def _rsc_object_block(rsc: str, object_key: str) -> str:
    match = re.search(rf'"{re.escape(object_key)}"\s*:\s*\{{([^{{}}]+)\}}', rsc)
    return match.group(1) if match else ""


def _rsc_find_from_block(block: str, key: str) -> str | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', block)
    return match.group(1) if match else None


def _rsc_combine_url_path(rsc: str, object_key: str) -> str | None:
    match = re.search(
        rf'"{re.escape(object_key)}"\s*:\s*\{{"url":"([^"]+)","path":"([^"]+)"',
        rsc,
    )
    if not match:
        return None
    return f"{match.group(1).rstrip('/')}/{match.group(2).lstrip('/')}"


def _extract_description_image_urls(description_contents: str) -> list[str]:
    seen: set[str] = set()
    image_urls: list[str] = []
    for url in re.findall(r'(?:data-src|src)="([^"]+)"', description_contents or ""):
        if not url or url.startswith("data:image/") or url in seen:
            continue
        seen.add(url)
        image_urls.append(url)
    return image_urls


def _parse_qna_count(payload: Any) -> int | None:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("qnaCount", "goodsQnACount", "count", "totalCount"):
                count = to_int(data.get(key))
                if count is not None:
                    return count
        for key in ("qnaCount", "goodsQnACount", "count", "totalCount"):
            count = to_int(payload.get(key))
            if count is not None:
                return count
    return None


def _json_compact(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _is_challenge_html(html: str) -> bool:
    lowered = html.lower()
    return "__cf_chl_opt" in lowered or "just a moment" in lowered or "잠시만 기다려" in html


def _build_detail_enrichment_payload(
    goods_no: str,
    html: str,
    rsc: str,
    extra_payload: dict[str, Any] | None,
    description_payload: dict[str, Any] | None,
    qna_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_data = ((extra_payload or {}).get("data") or {}) if isinstance(extra_payload, dict) else {}
    description_data = ((description_payload or {}).get("data") or {}) if isinstance(description_payload, dict) else {}
    review_info = (extra_data.get("reviewInfoDto") or {}) if isinstance(extra_data, dict) else {}
    option_list = (((extra_data.get("optionDetail") or {}).get("optionList")) or []) if isinstance(extra_data, dict) else []
    option_detail = option_list[0] if option_list else {}
    description_contents = description_data.get("descriptionContents") if isinstance(description_data, dict) else ""
    description_image_urls = _extract_description_image_urls(description_contents or "")
    standard_category_block = _rsc_object_block(rsc, "standardCategory")
    display_category_block = _rsc_object_block(rsc, "displayCategory")

    item_id = (
        _find_meta_content(html, "property", "eg:itemId")
        or option_detail.get("standardCode")
        or _rsc_find_string(rsc, "standardCode")
    )
    option_number = option_detail.get("optionNumber") or _rsc_find_string(rsc, "optionNumber")
    standard_code = option_detail.get("standardCode") or _rsc_find_string(rsc, "standardCode")
    option_name = option_detail.get("optionName") or _rsc_find_string(rsc, "optionName")
    option_image_url = _rsc_combine_url_path(rsc, "optionImage")

    detail_meta = {
        "item_id": item_id,
        "og_url": _find_meta_content(html, "property", "og:url"),
        "og_image_url": _find_meta_content(html, "property", "og:image"),
        "eg_item_url": _find_meta_content(html, "property", "eg:itemUrl"),
        "goods_type_code": _rsc_find_string(rsc, "goodsTypeCode"),
        "goods_section_code": _rsc_find_string(rsc, "goodsSectionCode"),
        "trade_code": _rsc_find_string(rsc, "tradeCode"),
        "delivery_policy_number": _rsc_find_string(rsc, "deliveryPolicyNumber"),
        "online_brand_code": _rsc_find_string(rsc, "onlineBrand"),
        "online_brand_name": _rsc_find_string(rsc, "onlineBrandName"),
        "online_brand_eng_name": _rsc_find_string(rsc, "onlineBrandEngName"),
        "brand_code": _rsc_find_string(rsc, "brand"),
        "supplier_code": _rsc_find_string(rsc, "supplier"),
        "supplier_name": _rsc_find_string(rsc, "supplierName"),
        "status_code": _rsc_find_string(rsc, "status"),
        "status_name": _rsc_find_string(rsc, "statusName"),
        "sold_out_flag": option_detail.get("soldOutFlag"),
        "registered_at": _rsc_find_string(rsc, "registeredDate"),
        "modified_at": _rsc_find_string(rsc, "modifiedDate"),
        "display_start_at": _rsc_find_string(rsc, "displayStartDatetime"),
        "display_end_at": _rsc_find_string(rsc, "displayEndDatetime"),
        "standard_category_upper_code": _rsc_find_from_block(standard_category_block, "upperCategory"),
        "standard_category_upper_name": _rsc_find_from_block(standard_category_block, "upperCategoryName"),
        "standard_category_middle_code": _rsc_find_from_block(standard_category_block, "middleCategory"),
        "standard_category_middle_name": _rsc_find_from_block(standard_category_block, "middleCategoryName"),
        "standard_category_lower_code": _rsc_find_from_block(standard_category_block, "lowerCategory"),
        "standard_category_lower_name": _rsc_find_from_block(standard_category_block, "lowerCategoryName"),
        "display_category_upper_number": _rsc_find_from_block(display_category_block, "upperCategoryNumber"),
        "display_category_upper_name": _rsc_find_from_block(display_category_block, "upperCategoryName"),
        "display_category_middle_number": _rsc_find_from_block(display_category_block, "middleCategoryNumber"),
        "display_category_middle_name": _rsc_find_from_block(display_category_block, "middleCategoryName"),
        "display_category_lower_number": _rsc_find_from_block(display_category_block, "lowerCategoryNumber"),
        "display_category_lower_name": _rsc_find_from_block(display_category_block, "lowerCategoryName"),
        "display_category_leaf_number": _rsc_find_from_block(display_category_block, "leafCategoryNumber"),
        "display_category_leaf_name": _rsc_find_from_block(display_category_block, "leafCategoryName"),
        "option_number": option_number,
        "option_name": option_name,
        "standard_code": standard_code,
        "option_image_url": option_image_url,
        "qna_count": _parse_qna_count(qna_payload),
        "description_type_code": description_data.get("descriptionTypeCode") if isinstance(description_data, dict) else None,
        "description_image_count": len(description_image_urls),
        "description_image_urls_json": _json_compact(description_image_urls),
        "detail_meta_json": _json_compact(
            {
                "goods_no": goods_no,
                "item_id": item_id,
                "meta": {
                    "og_url": _find_meta_content(html, "property", "og:url"),
                    "og_image_url": _find_meta_content(html, "property", "og:image"),
                    "eg_item_url": _find_meta_content(html, "property", "eg:itemUrl"),
                },
                "rsc_fields": {
                    "goodsTypeCode": _rsc_find_string(rsc, "goodsTypeCode"),
                    "goodsSectionCode": _rsc_find_string(rsc, "goodsSectionCode"),
                    "tradeCode": _rsc_find_string(rsc, "tradeCode"),
                    "deliveryPolicyNumber": _rsc_find_string(rsc, "deliveryPolicyNumber"),
                    "onlineBrand": _rsc_find_string(rsc, "onlineBrand"),
                    "brand": _rsc_find_string(rsc, "brand"),
                    "supplier": _rsc_find_string(rsc, "supplier"),
                    "supplierName": _rsc_find_string(rsc, "supplierName"),
                    "registeredDate": _rsc_find_string(rsc, "registeredDate"),
                    "modifiedDate": _rsc_find_string(rsc, "modifiedDate"),
                },
            }
        ),
        "extra_data_json": _json_compact(extra_data),
        "review_count": review_info.get("reviewCnt"),
        "rating": review_info.get("reviewAvgScore"),
    }
    if detail_meta["sold_out_flag"] is None:
        detail_meta["sold_out_flag"] = _rsc_find_bool(rsc, "soldOutFlag")
    return detail_meta


_detail_threads = threading.local()


def _get_detail_session(cffi_requests: Any, browser: str, supports_impersonate: bool, cookies: dict[str, str]):
    sessions = getattr(_detail_threads, "sessions", None)
    if sessions is None:
        sessions = {}
        _detail_threads.sessions = sessions
    if browser not in sessions:
        session = cffi_requests.Session(impersonate=browser) if supports_impersonate else cffi_requests.Session()
        if cookies:
            session.cookies.update(cookies)
        sessions[browser] = session
    return sessions[browser]


def fetch_detail_enrichment_http(
    goods_nos: list[str],
    *,
    detail_urls: dict[str, str] | None = None,
    playwright_cookies: dict[str, str] | None = None,
    max_workers: int = DETAIL_WORKERS,
    timeout: float = DETAIL_TIMEOUT,
    retries: int = DETAIL_RETRIES,
) -> dict[str, dict[str, Any]]:
    try:
        from curl_cffi import requests as cffi_requests
        supports_impersonate = True
    except ImportError:
        import requests as cffi_requests
        supports_impersonate = False

    unique_nos = list(dict.fromkeys([x for x in goods_nos if x]))
    if not unique_nos:
        return {}

    detail_url_map = detail_urls or {}
    base_cookies = playwright_cookies or {}
    print(
        f"[INFO] 상세 메타 수집: {len(unique_nos)}건 "
        f"(workers={max_workers}, timeout={timeout}, retries={retries})"
    )

    def _fetch_one(gno: str, worker_id: int) -> tuple[str, dict[str, Any]]:
        browser = _pick_browser(worker_id)
        session = _get_detail_session(cffi_requests, browser, supports_impersonate, base_cookies)
        html_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.oliveyoung.co.kr/store/main/getBestList.do",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        }
        api_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.oliveyoung.co.kr",
            "Referer": DETAIL_SHORT_URL.format(goods_no=gno),
            "User-Agent": html_headers["User-Agent"],
        }

        for attempt in range(1, retries + 1):
            try:
                detail_url = detail_url_map.get(gno) or DETAIL_SHORT_URL.format(goods_no=gno)
                detail_resp = session.get(
                    detail_url,
                    headers=html_headers,
                    timeout=timeout,
                    allow_redirects=True,
                )
                status = int(detail_resp.status_code)
                if status in (403, 429, 500, 502, 503):
                    time.sleep(_review_backoff(attempt))
                    continue
                if status != 200 or _is_challenge_html(detail_resp.text):
                    return gno, {}

                html = detail_resp.text
                rsc = _extract_rsc(html)
                extra_payload = None
                description_payload = None
                qna_payload = None

                extra_body = _build_extra_api_body(gno, rsc)
                if extra_body:
                    try:
                        extra_resp = session.post(
                            EXTRA_API,
                            json=extra_body,
                            headers=api_headers,
                            timeout=timeout,
                        )
                        if int(extra_resp.status_code) == 200:
                            extra_payload = extra_resp.json()
                    except Exception:
                        extra_payload = None

                try:
                    description_resp = session.get(
                        DESCRIPTION_API.format(goods_no=gno),
                        headers=api_headers,
                        timeout=timeout,
                    )
                    if int(description_resp.status_code) == 200:
                        description_payload = description_resp.json()
                except Exception:
                    description_payload = None

                try:
                    qna_resp = session.get(
                        QNA_API.format(goods_no=gno),
                        headers=api_headers,
                        timeout=timeout,
                    )
                    if int(qna_resp.status_code) == 200:
                        qna_payload = qna_resp.json()
                except Exception:
                    qna_payload = None

                return gno, _build_detail_enrichment_payload(
                    gno,
                    html,
                    rsc,
                    extra_payload,
                    description_payload,
                    qna_payload,
                )
            except Exception as exc:
                if _is_dns_error(exc):
                    return gno, {}
                time.sleep(_review_backoff(attempt))

        return gno, {}

    results: dict[str, dict[str, Any]] = {}
    worker_count = max(1, min(max_workers, len(unique_nos)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(_fetch_one, gno, idx): gno
            for idx, gno in enumerate(unique_nos)
        }
        for future in as_completed(futures):
            gno = futures[future]
            try:
                key, payload = future.result()
            except Exception:
                key, payload = gno, {}
            results[key] = payload or {}

    success = len([payload for payload in results.values() if payload])
    print(f"[INFO] 상세 메타 수집 완료: {success}/{len(unique_nos)}건 성공")
    return {gno: results.get(gno, {}) for gno in unique_nos}


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
    if not isinstance(payload, dict):
        return {}

    status = payload.get("status")
    if status is not None:
        if isinstance(status, bool):
            if not status:
                return {}
        elif isinstance(status, (int, float)):
            if int(status) != 1:
                return {}
        else:
            status_str = str(status).strip().lower()
            if status_str and status_str not in {"success", "ok", "successful", "true", "1", "s"}:
                return {}

    data = payload.get("data") or payload.get("result") or payload.get("body") or {}
    if not isinstance(data, dict):
        return {}

    # ratingDistribution 구조가 바뀐 경우/legacy 구조 모두 대응
    rating_distribution = data.get("ratingDistribution") or {}
    rating = (
        data.get("averageRating")
        or data.get("rating")
        or (rating_distribution.get("averageRating") if isinstance(rating_distribution, dict) else None)
    )

    # 다양한 API 스키마의 리뷰 건수 키 대응
    review_count = (
        data.get("reviewCount")
        or data.get("review_count")
        or data.get("reviewCnt")
        or data.get("review_total_count")
        or data.get("totalReviewCount")
    )

    if review_count is None and isinstance(rating_distribution, dict):
        review_count = (
            rating_distribution.get("reviewCount")
            or rating_distribution.get("totalCount")
            or rating_distribution.get("count")
        )

    # 여전히 값이 비어있으면 빈 응답으로 판단
    if review_count is None and rating is None:
        return {}

    if isinstance(review_count, str):
        review_count = to_int(review_count)

    return {
        "review_count": review_count,
        "rating": rating,
    }


def _parse_review_count_only(payload: Any) -> dict[str, Any] | None:
    """리뷰 카운트만 별도 엔드포인트에서 받는 응답을 정규화."""
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return None

    data = payload.get("data")
    if isinstance(data, dict):
        count = (
            data.get("count")
            or data.get("reviewCount")
            or data.get("totalCount")
            or data.get("reviewCountTotal")
        )
    else:
        count = payload.get("count") or payload.get("reviewCount") or payload.get("totalCount")

    if count is None:
        return None
    count = to_int(count)
    if count is None:
        return None
    return {"review_count": count}


def _is_dns_error(exc: BaseException) -> bool:
    message = str(exc)
    lowered = message.lower()
    return (
        "name or service not known" in lowered
        or "nameresolution" in lowered
        or "temporary failure in name resolution" in lowered
        or "nodename nor servname provided" in lowered
        or "getaddrinfo failed" in lowered
    )


def _resolve_host(host: str) -> bool:
    """필수 도메인 해석이 가능한지 사전 확인."""
    try:
        socket.gethostbyname(host)
        return True
    except Exception:
        return False


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
    try:
        from curl_cffi import requests as cffi_requests
        supports_impersonate = True
    except ImportError:
        import requests as cffi_requests
        supports_impersonate = False

    def _new_session(browser: str):
        if supports_impersonate:
            return cffi_requests.Session(impersonate=browser)
        return cffi_requests.Session()

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
        alt_url = f"{ALT_REVIEW_API}?product-id={gno}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://m.oliveyoung.co.kr/",
            "Origin": "https://m.oliveyoung.co.kr",
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
                "Mobile/15E148 Safari/604.1"
            ),
            "sec-ch-ua": '"Chromium";v="120", "Not A;Brand";v="99", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"iOS"',
        }
        if supports_impersonate:
            session = _get_review_session(cffi_requests, browser)
        else:
            session = _new_session(browser)

        for attempt in range(1, retries + 1):
            try:
                resp = session.get(url, headers=headers, timeout=timeout)
                status = int(resp.status_code)
                if status == 200:
                    parsed = _parse_review_payload(resp.json())
                    if parsed:
                        return gno, parsed
                    # 메인 API에서 실패한 경우 대체 카운트 API로 보완 시도
                    alt_resp = session.get(alt_url, headers=headers, timeout=timeout)
                    if int(alt_resp.status_code) == 200:
                        parsed_alt = _parse_review_count_only(alt_resp.json())
                        if parsed_alt:
                            return gno, parsed_alt
                if status in (403, 429, 500, 502, 503):
                    time.sleep(_review_backoff(attempt))
                    continue
                return gno, None
            except Exception as exc:
                if _is_dns_error(exc):
                    print(f"[ERROR] DNS/네트워크 해석 실패: {gno}, {exc}")
                    # DNS는 즉시 반환하여 전체 재시도 루프를 줄임
                    return gno, None
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


def _crawl_with_playwright(args: argparse.Namespace, user_data_dir: Path):
    """playwright 폴백용 context manager 래퍼."""
    from contextlib import contextmanager
    if _SYNC_PLAYWRIGHT is None:
        raise RuntimeError("playwright sync API 미사용 가능 상태입니다.")

    @contextmanager
    def _ctx():
        with _SYNC_PLAYWRIGHT() as pw:
            import platform
            headless_mode = not args.headed
            launch_kwargs = {
                "user_data_dir": str(user_data_dir),
                "headless": headless_mode,
                "no_viewport": True,
                "locale": "ko-KR",
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1920,1080",
                    "--disable-crash-reporter",
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
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z"
    out_dir = Path(os.getcwd()) / args.out_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    playwright_cookies: dict[str, str] = {}

    if args.review_only:
        print("[INFO] review-only 모드: 카테고리 수집 없이 리뷰 API만 호출")
        if not _resolve_host("m.oliveyoung.co.kr") or not _resolve_host("product-review-service.oliveyoung.com"):
            print("ERROR: oliveyoung 도메인 DNS 해석 실패. 현재 실행 환경의 네트워크/프록시/DNS를 점검하세요.", file=sys.stderr)
            print("진단 예시: nslookup m.oliveyoung.co.kr", file=sys.stderr)
            print("  또는 requests/브라우저가 동일하게 실패하면", file=sys.stderr)
            print("  DNS/방화벽/프록시 레벨 차단 가능성이 큽니다.", file=sys.stderr)
            sys.exit(1)
        goods_raw = [g.strip() for g in args.review_goods.split(",") if g.strip()]
        if not goods_raw:
            print("ERROR: --review-goods가 비어 있습니다. 쉼표로 구분된 goods_no를 전달하세요.", file=sys.stderr)
            print("예: --review-goods 100000001,100000002", file=sys.stderr)
            sys.exit(1)

        review_stats = fetch_review_stats_http(
            goods_raw,
            max_workers=args.review_workers,
            timeout=args.review_timeout,
            retries=args.review_retries,
        )

        rows = []
        review_total = len(goods_raw)
        review_success = len([v for v in review_stats.values() if v])
        review_fail = review_total - review_success
        review_summary = {
            "total": review_total,
            "cacheHit": 0,
            "cacheHitRate": 0,
            "success": review_success,
            "fail": review_fail,
            "rate": round(review_success / review_total * 100, 1) if review_total else 0,
        }

        for g in goods_raw:
            stats = review_stats.get(g, {})
            rows.append({
                "collected_at_utc": now_iso(),
                "category_code": "REVIEW",
                "category_name": "review_only",
                "rank": None,
                "goods_no": g,
                "detail_disp_cat_no": "",
                "detail_url": f"https://m.oliveyoung.co.kr/store/goods/getGoodsDetail.do?goodsNo={g}",
                "brand_name": "",
                "product_name": "",
                "original_price": None,
                "discount_price": None,
                "discount_rate": None,
                "ranking_tags": "review_only",
                "review_count": stats.get("review_count"),
                "rating": stats.get("rating"),
            })

        _write_output(
            out_dir,
            rows,
            [],
            args,
            run_id,
            review_summary=review_summary,
            browser_engine="review-only",
        )
        print(
            f"[INFO] 리뷰 단독 테스트 완료: success={review_success}, fail={review_fail}, "
            f"rate={review_summary['rate']}%"
        )
        return

    browser_engine = args.browser_engine

    if browser_engine == "auto":
        if USE_CAMOUFOX:
            browser_engine = "camoufox"
        elif _SYNC_PLAYWRIGHT is not None:
            browser_engine = "playwright"
        else:
            browser_engine = "none"
    elif browser_engine == "camoufox" and not USE_CAMOUFOX:
        print("ERROR: --browser-engine=camoufox 선택이지만 camoufox가 미설치입니다.", file=sys.stderr)
        print("  pip install camoufox 로 설치하세요.", file=sys.stderr)
        sys.exit(1)
    elif browser_engine == "playwright" and _SYNC_PLAYWRIGHT is None:
        print(
            "ERROR: --browser-engine=playwright 선택이지만 patchright/playwright가 미설치입니다.",
            file=sys.stderr,
        )
        print("  pip install patchright 또는 playwright 로 설치하세요.", file=sys.stderr)
        sys.exit(1)

    if browser_engine == "none":
        print("ERROR: camoufox / patchright / playwright 모두 미설치.", file=sys.stderr)
        print("  pip install camoufox 또는 patchright 또는 playwright", file=sys.stderr)
        sys.exit(1)

    if browser_engine == "camoufox":
        print("[INFO] 브라우저 엔진: camoufox (Firefox + BrowserForge)")
    else:
        print("[INFO] 브라우저 엔진: playwright (chromium)")

    # persistent context용 user_data_dir (브라우저 엔진별 분리)
    profile_name = ".browser_profile_camoufox" if browser_engine == "camoufox" else ".browser_profile_playwright"
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

    if args.dry_run:
        print("[WARN] dry-run 모드: 브라우저 호출 없이 목업 데이터 생성")
        category_count = min(args.category_limit, len(DEFAULT_CATEGORIES)) if args.category_limit > 0 else 0
        row_repeat = max(0, args.dry_run_rows)
        for idx in range(category_count):
            category = DEFAULT_CATEGORIES[idx]
            for rank in range(1, row_repeat + 1):
                rows.append({
                    "collected_at_utc": now_iso(),
                    "category_code": category["code"],
                    "category_name": category["name"],
                    "rank": rank,
                    "goods_no": f"DRY-{category['code']}-{rank:03d}",
                    "detail_disp_cat_no": category.get("disp_cat_no") or "",
                    "detail_url": f"https://www.oliveyoung.co.kr/{category['code']}",
                    "brand_name": "dry-run",
                    "product_name": f"목업상품-{category['name']}-{rank}",
                    "original_price": 1000 * rank,
                    "discount_price": 800 * rank,
                    "discount_rate": None,
                    "ranking_tags": "dry-run",
                    "review_count": None,
                    "rating": None,
                })
            category_summaries.append({
                "categoryCode": category["code"],
                "categoryName": category["name"],
                "challengeDetected": False,
                "goodsCount": row_repeat,
                "rankMin": 1 if row_repeat > 0 else None,
                "rankMax": row_repeat if row_repeat > 0 else None,
                "urlAfterClick": args.url,
            })

        review_summary = {
            "total": len(rows),
            "cacheHit": 0,
            "cacheHitRate": 0,
            "success": 0,
            "fail": len(rows),
            "rate": 0.0,
        }
        _write_output(
            out_dir,
            rows,
            category_summaries,
            args,
            run_id,
            review_summary=review_summary,
            browser_engine=browser_engine,
        )
        if len(rows) < args.min_rows:
            print(
                f"[WARN] dry-run 건수({len(rows)})가 min_rows({args.min_rows}) 미만. "
                "실제 모드에서는 최소 조건 재설정이 필요합니다."
            )
        return

    if not _resolve_host("www.oliveyoung.co.kr"):
        print("ERROR: www.oliveyoung.co.kr DNS 해석 실패. 현재 실행 환경에서 사이트 도메인 접속이 안 됩니다.", file=sys.stderr)
        print("실행 환경 DNS/방화벽/네트워크 라우팅을 먼저 점검하세요.", file=sys.stderr)
        sys.exit(1)

    # 브라우저 엔진 선택
    if browser_engine == "camoufox":
        browser_ctx = _crawl_with_camoufox(args, user_data_dir)
    else:
        browser_ctx = _crawl_with_playwright(args, user_data_dir)

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
                _write_output(
                    out_dir,
                    rows,
                    category_summaries,
                    args,
                    run_id,
                    browser_engine=browser_engine,
                )
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
                _write_output(
                    out_dir,
                    rows,
                    category_summaries,
                    args,
                    run_id,
                    browser_engine=browser_engine,
                )
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
                try:
                    playwright_cookies = {
                        cookie["name"]: cookie["value"]
                        for cookie in context.cookies()
                        if cookie.get("name") and cookie.get("value")
                    }
                except Exception:
                    playwright_cookies = {}
                page.close()
            except Exception:
                pass

    except Exception:
        _write_output(
            out_dir,
            rows,
            category_summaries,
            args,
            run_id,
            browser_engine=browser_engine,
        )
        raise

    # 리뷰 통계 API 호출 (curl_cffi HTTP, 브라우저 세션 불필요)
    unique_goods_nos = list({r["goods_no"] for r in rows if r.get("goods_no")})
    detail_url_map = {
        row["goods_no"]: row["detail_url"]
        for row in rows
        if row.get("goods_no") and row.get("detail_url")
    }
    detail_enrichment = fetch_detail_enrichment_http(
        unique_goods_nos,
        detail_urls=detail_url_map,
        playwright_cookies=playwright_cookies,
        max_workers=args.detail_workers,
        timeout=args.detail_timeout,
        retries=args.detail_retries,
    )
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
        detail = detail_enrichment.get(gno, {})
        for key, value in detail.items():
            if key in {"review_count", "rating"}:
                continue
            row[key] = value
        stats = review_stats.get(gno, {})
        row["review_count"] = stats.get("review_count") if stats.get("review_count") is not None else detail.get("review_count")
        row["rating"] = stats.get("rating") if stats.get("rating") is not None else detail.get("rating")

    review_summary = {
        "total": review_total,
        "cacheHit": cache_hit_count,
        "cacheHitRate": cache_hit_rate,
        "success": review_success,
        "fail": review_fail,
        "rate": round(review_success / review_total * 100, 1) if review_total else 0,
    }
    _write_output(
        out_dir,
        rows,
        category_summaries,
        args,
        run_id,
        review_summary=review_summary,
        browser_engine=browser_engine,
    )

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
    browser_engine: str = "unknown",
) -> None:
    """파일 출력 (성공/실패 모두 기록)."""
    csv_headers = [
        "collected_at_utc", "category_code", "category_name", "rank",
        "goods_no", "detail_disp_cat_no", "detail_url", "brand_name",
        "product_name", "original_price", "discount_price", "discount_rate",
        "ranking_tags",
        "review_count", "rating",
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

    csv_path = str(out_dir / "ranking_rows.csv")
    json_path = str(out_dir / "ranking_rows.json")
    summary_path = str(out_dir / "summary.json")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        f.write(to_csv(rows, csv_headers))

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    summary = {
        "runId": run_id,
        "browserEngine": browser_engine,
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
