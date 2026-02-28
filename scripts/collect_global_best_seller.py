#!/usr/bin/env python3
"""Collect global Olive Young Best Sellers (Top Orders + Top in Korea)."""

from __future__ import annotations

import argparse
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


TAB_NAME = {
    1: "Top Orders",
    2: "Top in Korea",
}

DEFAULT_ORDER_ALLOWLIST = [
    "1000000001",
    "1000000003",
    "1000000008",
    "1000000011",
    "1000000031",
    "1000000052",
    "1000000070",
    "1000000087",
    "1000000095",
    "1000000134",
    "1000000137",
    "1000000162",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_int_set(raw: str | None) -> set[int]:
    if not raw:
        return set()
    out: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if token.isdigit():
            out.add(int(token))
    return out


def parse_str_set(raw: str | None) -> set[str]:
    if not raw:
        return set()
    out: set[str] = set()
    for token in raw.split(","):
        token = token.strip()
        if token:
            out.add(token)
    return out


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text.replace(",", "")))
    except ValueError:
        return None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def calc_discount_rate(original_price: float | None, sale_price: float | None) -> int | None:
    if original_price is None or sale_price is None or original_price <= 0:
        return None
    if sale_price >= original_price:
        return None
    return round(((original_price - sale_price) * 100) / original_price)


@dataclass
class BestPageContext:
    preview_date: str
    enc_key: str
    enc_text: str
    aces_cntry_code: str
    acc_param: str
    disp_page_type_code: str
    lang_code: str
    disp_page_no: str
    mrgn_cntry: str
    dlv_cntry_code: str
    order_category_allowlist: list[str]


@dataclass
class RankCategory:
    tab_no: int
    tab_name: str
    category_no: str
    category_name: str
    api_category_value: str


@dataclass
class ReviewMetric:
    review_count_total: int | None
    rating_fallback: float | None
    global_review_count: int | None
    domestic_review_count: int | None
    global_star_rate: float | None


class GlobalBestCrawler:
    def __init__(
        self,
        *,
        base_url: str,
        ranking_api_endpoint: str,
        target_url: str,
        timeout_sec: int,
        retries: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.ranking_api_endpoint = ranking_api_endpoint.rstrip("/")
        self.target_url = target_url
        self.timeout_sec = timeout_sec
        self.retries = retries

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": self.target_url,
                "Origin": self.base_url,
            }
        )

    def close(self) -> None:
        self.session.close()

    def _get(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_sec)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(min(1.5 * attempt, 5.0))
        raise RuntimeError(f"GET failed: {url}, params={params}, error={last_error}") from last_error

    def _post(self, url: str, *, payload: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.post(url, json=payload, timeout=self.timeout_sec)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(min(1.5 * attempt, 5.0))
        raise RuntimeError(f"POST failed: {url}, payload={payload}, error={last_error}") from last_error

    def _get_text(self, url: str) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout_sec)
                response.raise_for_status()
                return response.text
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(min(1.5 * attempt, 5.0))
        raise RuntimeError(f"GET text failed: {url}, error={last_error}") from last_error

    def fetch_context(self) -> BestPageContext:
        html_text = self._get_text(self.target_url)
        decoded = html.unescape(html_text)

        id_value_pairs = dict(
            re.findall(
                r'<input[^>]+id="([^"]+)"[^>]*value="([^"]*)"',
                decoded,
                flags=re.IGNORECASE,
            )
        )

        allowlist = DEFAULT_ORDER_ALLOWLIST
        match = re.search(
            r"JSON\.parse\('(\[[^']+\])'\)\.includes\(ctgr\.ctgrNo\)",
            decoded,
        )
        if match:
            try:
                parsed = json.loads(match.group(1))
                if isinstance(parsed, list) and parsed:
                    allowlist = [str(x) for x in parsed]
            except json.JSONDecodeError:
                pass

        return BestPageContext(
            preview_date=id_value_pairs.get("previewDate", ""),
            enc_key=id_value_pairs.get("encKey", ""),
            enc_text=id_value_pairs.get("encText", ""),
            aces_cntry_code=id_value_pairs.get("acesCntryCode", "00"),
            acc_param=id_value_pairs.get("accParam", ""),
            disp_page_type_code=id_value_pairs.get("dispPageTypeCode", "30"),
            lang_code=id_value_pairs.get("langCode", "en"),
            disp_page_no=id_value_pairs.get("dispPageNo", ""),
            mrgn_cntry=id_value_pairs.get("mrgnCntry", "9999"),
            dlv_cntry_code=id_value_pairs.get("dlvCntryCode", "1230"),
            order_category_allowlist=allowlist,
        )

    def fetch_order_categories(
        self,
        ctx: BestPageContext,
        *,
        include_hidden: bool,
    ) -> list[RankCategory]:
        payload = self._get(
            f"{self.base_url}/display/page/best-seller-data",
            params={
                "previewDate": ctx.preview_date,
                "encKey": ctx.enc_key,
                "encText": ctx.enc_text,
                "acesCntryCode": ctx.aces_cntry_code,
                "accParam": ctx.acc_param,
                "dispPageTypeCode": ctx.disp_page_type_code,
                "langCode": ctx.lang_code,
                "dispPageNo": ctx.disp_page_no,
                "mrgnCntryCode": ctx.mrgn_cntry,
                "dlvCntryCode": ctx.dlv_cntry_code,
            },
        )
        raw_list = payload.get("ctgrList") or []
        categories: list[RankCategory] = [
            RankCategory(
                tab_no=1,
                tab_name=TAB_NAME[1],
                category_no="ALL",
                category_name="All",
                api_category_value="",
            )
        ]

        for item in raw_list:
            if not isinstance(item, dict):
                continue
            ctgr_no = str(item.get("ctgrNo") or "").strip()
            ctgr_name = str(item.get("ctgrName") or "").strip() or ctgr_no
            total_count = to_int(item.get("totalCount")) or 0
            if not ctgr_no:
                continue
            if not include_hidden:
                if ctgr_no not in ctx.order_category_allowlist:
                    continue
                if total_count <= 52:
                    continue
            categories.append(
                RankCategory(
                    tab_no=1,
                    tab_name=TAB_NAME[1],
                    category_no=ctgr_no,
                    category_name=ctgr_name,
                    api_category_value=ctgr_no,
                )
            )
        return categories

    def fetch_order_products(self, ctx: BestPageContext, category_api_value: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "ctgrNo": category_api_value,
            "acesCntryCode": ctx.aces_cntry_code,
            "previewDate": ctx.preview_date,
            "encKey": ctx.enc_key,
            "encText": ctx.enc_text,
            "accParam": ctx.acc_param,
            "dispPageTypeCode": ctx.disp_page_type_code,
            "langCode": ctx.lang_code,
            "dispPageNo": ctx.disp_page_no,
            "mrgnCntryCode": ctx.mrgn_cntry,
            "dlvCntryCode": ctx.dlv_cntry_code,
            "isGlobal": "true",
            "showSoldoutProduct": "true",
        }
        payload = self._get(f"{self.base_url}/display/product/best-seller/order-best", params=params)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def fetch_korea_categories(self, ctx: BestPageContext) -> list[RankCategory]:
        payload = self._get(
            f"{self.ranking_api_endpoint}/v1/pages/ranking/sales/categories",
            params={
                "region": "KR",
                "language-code": ctx.lang_code,
            },
        )
        raw_list = payload.get("data", {}).get("pages.ranking.categories") or []
        categories: list[RankCategory] = [
            RankCategory(
                tab_no=2,
                tab_name=TAB_NAME[2],
                category_no="ALL",
                category_name="All",
                api_category_value="1000000001",
            )
        ]
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            cat_id = str(item.get("id") or "").strip()
            cat_name = str(item.get("name") or "").strip() or cat_id
            if not cat_id or cat_id == "1000000001":
                continue
            categories.append(
                RankCategory(
                    tab_no=2,
                    tab_name=TAB_NAME[2],
                    category_no=cat_id,
                    category_name=cat_name,
                    api_category_value=cat_id,
                )
            )
        return categories

    def fetch_korea_products(self, ctx: BestPageContext, category_api_value: str) -> list[dict[str, Any]]:
        payload = self._get(
            f"{self.ranking_api_endpoint}/v1/pages/ranking/sales/products",
            params={
                "category-id": category_api_value,
                "region": "KR",
                "language-code": ctx.lang_code,
                "margin-country-code": ctx.mrgn_cntry,
                "delivery-country-code": ctx.dlv_cntry_code,
            },
        )
        raw_list = payload.get("data", {}).get("pages.ranking.products") or []
        return [item for item in raw_list if isinstance(item, dict)]

    def fetch_global_review_summary(self, prdt_no: str) -> tuple[int | None, float | None]:
        payload = self._post(
            f"{self.base_url}/product/review-summary",
            payload={
                "filterYn": "N",
                "prdtNo": prdt_no,
                "prdtGbnCode": "10",
                "movReviewYn": "N",
                "photoReviewYn": "N",
                "optnYn": "N",
                "transUseYn": "N",
                "filterUseYn": "N",
            },
        )
        if not isinstance(payload, dict):
            return None, None
        return to_int(payload.get("totalReviewCount")), to_float(payload.get("totalStarRate"))

    def fetch_domestic_review_count(self, prdt_no: str) -> int | None:
        payload = self._get(
            "https://product-review-service.oliveyoung.com/api/v1/domestic-reviews/count",
            params={"product-id": prdt_no},
        )
        if isinstance(payload, dict):
            return to_int(payload.get("count"))
        return to_int(payload)


def normalize_order_item(
    item: dict[str, Any],
    *,
    rank: int,
    collected_at_utc: str,
    category: RankCategory,
    base_url: str,
) -> dict[str, Any] | None:
    prdt_no = str(item.get("prdtNo") or "").strip()
    if not prdt_no:
        return None
    original_price = to_float(item.get("nrmlAmt"))
    sale_price = to_float(item.get("saleAmt"))
    discount_rate = to_int(item.get("eventSlprcDscntRt"))
    if discount_rate is None:
        discount_rate = calc_discount_rate(original_price, sale_price)

    tags: list[str] = []
    if str(item.get("cpnYn") or "").upper() == "Y":
        tags.append("coupon")
    if str(item.get("giftYn") or "").upper() == "Y":
        tags.append("gift")
    if str(item.get("bestYn") or "").upper() == "Y":
        tags.append("best")

    image_path = str(item.get("imagePath") or "").strip()
    image_url = f"https://cdn-image.oliveyoung.com/{image_path}" if image_path else None

    return {
        "collected_at_utc": collected_at_utc,
        "tab_no": category.tab_no,
        "tab_name": category.tab_name,
        "category_no": category.category_no,
        "category_name": category.category_name,
        "rank": rank,
        "product_no": prdt_no,
        "brand_name": item.get("brandName"),
        "product_name": item.get("prdtName"),
        "rating": to_float(item.get("avgScore")),
        "review_count": to_int(item.get("reviewCnt")),
        "original_price": original_price,
        "sale_price": sale_price,
        "discount_rate": discount_rate,
        "sell_status_code": item.get("sellStatCode"),
        "tags": ",".join(tags),
        "detail_url": f"{base_url}/product/detail?prdtNo={prdt_no}",
        "image_url": image_url,
    }


def normalize_korea_item(
    item: dict[str, Any],
    *,
    rank: int,
    collected_at_utc: str,
    category: RankCategory,
    base_url: str,
) -> dict[str, Any] | None:
    prdt_no = str(item.get("id") or "").strip()
    if not prdt_no:
        return None
    original_price = to_float(item.get("original_price"))
    sale_price_raw = to_float(item.get("sale_price"))
    max_discount_price = to_float(item.get("max_discount_price"))
    sale_price = (
        max_discount_price
        if max_discount_price is not None and max_discount_price > 0
        else sale_price_raw
    )
    discount_rate = calc_discount_rate(original_price, sale_price)

    tags: list[str] = []
    if bool(item.get("has_coupon")):
        tags.append("coupon")
    if bool(item.get("has_gift")):
        tags.append("gift")
    if str(item.get("promotion_name") or "").strip():
        tags.append("promotion")
    if bool(item.get("is_soldout")):
        tags.append("sold_out")

    image_path = str(item.get("thumbnail_img_url") or "").strip()
    image_url = f"https://cdn-image.oliveyoung.com/{image_path}" if image_path else None

    return {
        "collected_at_utc": collected_at_utc,
        "tab_no": category.tab_no,
        "tab_name": category.tab_name,
        "category_no": category.category_no,
        "category_name": category.category_name,
        "rank": rank,
        "product_no": prdt_no,
        "brand_name": item.get("brand_name"),
        "product_name": item.get("name"),
        "rating": to_float(item.get("rate")),
        "review_count": to_int(item.get("review_count")),
        "original_price": original_price,
        "sale_price": sale_price,
        "discount_rate": discount_rate,
        "sell_status_code": "00" if bool(item.get("is_soldout")) else "10",
        "tags": ",".join(tags),
        "detail_url": f"{base_url}/product/detail?prdtNo={prdt_no}",
        "image_url": image_url,
    }


def dedupe_and_sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows:
        key = (int(row["rank"]), str(row["product_no"]))
        if key not in by_key:
            by_key[key] = row
    return sorted(by_key.values(), key=lambda x: int(x["rank"]))


def build_category_summary(
    *,
    category: RankCategory,
    rows: list[dict[str, Any]],
    min_rank: int,
    max_rank: int,
) -> dict[str, Any]:
    ranks = sorted({int(r["rank"]) for r in rows if r.get("rank") is not None})
    missing = [r for r in range(min_rank, max_rank + 1) if r not in ranks]
    return {
        "tab_no": category.tab_no,
        "tab_name": category.tab_name,
        "category_no": category.category_no,
        "category_name": category.category_name,
        "collected_count": len(rows),
        "min_rank_collected": min(ranks) if ranks else None,
        "max_rank_collected": max(ranks) if ranks else None,
        "missing_rank_count": len(missing),
        "missing_ranks_preview": missing[:20],
    }


def append_tag(existing_tags: str | None, new_tag: str) -> str:
    parts = [p.strip() for p in (existing_tags or "").split(",") if p.strip()]
    if new_tag not in parts:
        parts.append(new_tag)
    return ",".join(parts)


def enrich_missing_review_metrics(
    rows: list[dict[str, Any]],
    crawler: GlobalBestCrawler,
    *,
    delay_sec: float,
    max_workers: int,
) -> dict[str, Any]:
    target_ids = sorted(
        {
            str(row.get("product_no"))
            for row in rows
            if row.get("product_no")
            and (
                row.get("review_count") in (None, 0)
                or row.get("rating") in (None, 0, 0.0)
            )
        }
    )
    metrics: dict[str, ReviewMetric] = {}

    stats = {
        "enabled": True,
        "target_product_count": len(target_ids),
        "worker_count": max(1, int(max_workers)),
        "product_api_success_count": 0,
        "product_api_error_count": 0,
        "rows_review_count_updated": 0,
        "rows_rating_updated": 0,
    }

    def fetch_one_product(product_no: str) -> tuple[str, ReviewMetric, bool]:
        global_count = None
        global_rate = None
        domestic_count = None
        had_error = False

        try:
            global_count, global_rate = crawler.fetch_global_review_summary(product_no)
        except Exception:  # noqa: BLE001
            had_error = True
        if delay_sec > 0:
            time.sleep(delay_sec)

        try:
            domestic_count = crawler.fetch_domestic_review_count(product_no)
        except Exception:  # noqa: BLE001
            had_error = True
        if delay_sec > 0:
            time.sleep(delay_sec)

        counts = [c for c in (global_count, domestic_count) if c is not None]
        review_count_total = sum(counts) if counts else None
        rating_fallback = global_rate if global_rate is not None and global_rate > 0 else None
        metric = ReviewMetric(
            review_count_total=review_count_total,
            rating_fallback=rating_fallback,
            global_review_count=global_count,
            domestic_review_count=domestic_count,
            global_star_rate=global_rate,
        )
        return product_no, metric, had_error

    if stats["worker_count"] <= 1:
        for product_no in target_ids:
            _, metric, had_error = fetch_one_product(product_no)
            metrics[product_no] = metric
            if had_error:
                stats["product_api_error_count"] += 1
            else:
                stats["product_api_success_count"] += 1
    else:
        with ThreadPoolExecutor(max_workers=stats["worker_count"]) as executor:
            future_map = {
                executor.submit(fetch_one_product, product_no): product_no
                for product_no in target_ids
            }
            for future in as_completed(future_map):
                product_no = future_map[future]
                try:
                    _, metric, had_error = future.result()
                except Exception:  # noqa: BLE001
                    metric = ReviewMetric(
                        review_count_total=None,
                        rating_fallback=None,
                        global_review_count=None,
                        domestic_review_count=None,
                        global_star_rate=None,
                    )
                    had_error = True
                metrics[product_no] = metric
                if had_error:
                    stats["product_api_error_count"] += 1
                else:
                    stats["product_api_success_count"] += 1

    for row in rows:
        product_no = str(row.get("product_no") or "")
        metric = metrics.get(product_no)
        if metric is None:
            continue

        current_review = row.get("review_count")
        if metric.review_count_total is not None:
            if current_review is None or (to_int(current_review) or 0) < metric.review_count_total:
                row["review_count"] = metric.review_count_total
                row["tags"] = append_tag(row.get("tags"), "review_count_enriched")
                stats["rows_review_count_updated"] += 1

        current_rating = row.get("rating")
        current_rating_num = to_float(current_rating)
        if metric.rating_fallback is not None and (current_rating_num is None or current_rating_num <= 0):
            row["rating"] = metric.rating_fallback
            row["tags"] = append_tag(row.get("tags"), "rating_enriched")
            stats["rows_rating_updated"] += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect global Olive Young best sellers for all categories."
    )
    parser.add_argument(
        "--base-url",
        default="https://global.oliveyoung.com",
        help="Global Olive Young base URL.",
    )
    parser.add_argument(
        "--target-url",
        default="https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1",
        help="Best seller page URL.",
    )
    parser.add_argument(
        "--ranking-api-endpoint",
        default="https://product-ranking-service.oliveyoung.com",
        help="Ranking API endpoint used for Top in Korea.",
    )
    parser.add_argument(
        "--out-dir",
        default="output/global_best_seller",
        help="Output directory.",
    )
    parser.add_argument(
        "--tabs",
        default="1,2",
        help="Comma-separated tabs. 1=Top Orders, 2=Top in Korea",
    )
    parser.add_argument(
        "--categories",
        default="",
        help="Optional category filter (category_no or api id), comma-separated.",
    )
    parser.add_argument(
        "--category-limit",
        type=int,
        default=0,
        help="Limit categories per tab for testing (0 = no limit).",
    )
    parser.add_argument(
        "--min-rank",
        type=int,
        default=1,
        help="Minimum rank to keep.",
    )
    parser.add_argument(
        "--max-rank",
        type=int,
        default=100,
        help="Maximum rank to keep.",
    )
    parser.add_argument(
        "--include-hidden-order-categories",
        action="store_true",
        help="Include hidden Top Orders categories (e.g., low-count categories).",
    )
    parser.add_argument(
        "--enrich-missing-review-metrics",
        action="store_true",
        help=(
            "Fill missing review_count/rating by querying product review APIs "
            "(slower, extra API calls)."
        ),
    )
    parser.add_argument(
        "--enrich-delay-sec",
        type=float,
        default=0.05,
        help="Delay between enrichment API calls (used with --enrich-missing-review-metrics).",
    )
    parser.add_argument(
        "--enrich-workers",
        type=int,
        default=1,
        help="Concurrent workers for enrichment API calls (1 = sequential).",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retries for API calls.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = now_tag()
    collected_at = now_iso()
    tabs = sorted(parse_int_set(args.tabs))
    if not tabs:
        raise SystemExit("No valid tabs. Use --tabs 1,2")
    category_filter = parse_str_set(args.categories)

    output_dir = Path(args.out_dir).expanduser().resolve() / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    crawler = GlobalBestCrawler(
        base_url=args.base_url,
        ranking_api_endpoint=args.ranking_api_endpoint,
        target_url=args.target_url,
        timeout_sec=args.timeout_sec,
        retries=args.retries,
    )

    all_rows: list[dict[str, Any]] = []
    category_summaries: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    enrichment_stats: dict[str, Any] = {"enabled": False}

    try:
        context = crawler.fetch_context()

        tab_categories: dict[int, list[RankCategory]] = {}
        if 1 in tabs:
            tab_categories[1] = crawler.fetch_order_categories(
                context,
                include_hidden=args.include_hidden_order_categories,
            )
        if 2 in tabs:
            tab_categories[2] = crawler.fetch_korea_categories(context)

        for tab_no in tabs:
            categories = tab_categories.get(tab_no, [])
            if category_filter:
                categories = [
                    cat
                    for cat in categories
                    if cat.category_no in category_filter
                    or cat.api_category_value in category_filter
                ]
            if args.category_limit > 0:
                categories = categories[: args.category_limit]

            for category in categories:
                try:
                    if tab_no == 1:
                        raw_items = crawler.fetch_order_products(context, category.api_category_value)
                        normalized = []
                        for idx, item in enumerate(raw_items, start=1):
                            row = normalize_order_item(
                                item,
                                rank=idx,
                                collected_at_utc=collected_at,
                                category=category,
                                base_url=args.base_url,
                            )
                            if row is None:
                                continue
                            if not (args.min_rank <= int(row["rank"]) <= args.max_rank):
                                continue
                            normalized.append(row)
                    else:
                        raw_items = crawler.fetch_korea_products(context, category.api_category_value)
                        normalized = []
                        for idx, item in enumerate(raw_items, start=1):
                            row = normalize_korea_item(
                                item,
                                rank=idx,
                                collected_at_utc=collected_at,
                                category=category,
                                base_url=args.base_url,
                            )
                            if row is None:
                                continue
                            if not (args.min_rank <= int(row["rank"]) <= args.max_rank):
                                continue
                            normalized.append(row)

                    deduped = dedupe_and_sort_rows(normalized)
                    all_rows.extend(deduped)
                    category_summaries.append(
                        build_category_summary(
                            category=category,
                            rows=deduped,
                            min_rank=args.min_rank,
                            max_rank=args.max_rank,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        {
                            "tab_no": tab_no,
                            "tab_name": TAB_NAME.get(tab_no, f"Tab-{tab_no}"),
                            "category_no": category.category_no,
                            "category_name": category.category_name,
                            "error": str(exc),
                        }
                    )
        if args.enrich_missing_review_metrics and all_rows:
            enrichment_stats = enrich_missing_review_metrics(
                all_rows,
                crawler,
                delay_sec=max(0.0, args.enrich_delay_sec),
                max_workers=max(1, args.enrich_workers),
            )
    finally:
        crawler.close()

    json_path = output_dir / "global_best_seller_rows.json"
    summary_path = output_dir / "summary.json"

    with json_path.open("w", encoding="utf-8") as fp:
        json.dump(all_rows, fp, ensure_ascii=False, indent=2)

    with summary_path.open("w", encoding="utf-8") as fp:
        json.dump(
            {
                "run_id": run_id,
                "collected_at_utc": collected_at,
                "base_url": args.base_url,
                "target_url": args.target_url,
                "tabs": tabs,
                "category_filter": sorted(category_filter),
                "row_count": len(all_rows),
                "category_summaries": category_summaries,
                "errors": errors,
                "enrichment": enrichment_stats,
                "api_endpoints": {
                    "order_categories": "/display/page/best-seller-data",
                    "order_products": "/display/product/best-seller/order-best",
                    "korea_categories": "/v1/pages/ranking/sales/categories",
                    "korea_products": "/v1/pages/ranking/sales/products",
                },
            },
            fp,
            ensure_ascii=False,
            indent=2,
        )

    print(f"[OK] rows={len(all_rows)}")
    print(f"[OK] json={json_path}")
    print(f"[OK] summary={summary_path}")
    if errors:
        print(f"[WARN] category_errors={len(errors)}")
    if not all_rows:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
