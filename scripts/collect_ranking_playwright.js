#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

function parseArgs(argv) {
  const args = {
    url: "https://www.oliveyoung.co.kr/store/main/getBestList.do",
    outDir: "output/ranking_playwright",
    categoryLimit: 20,
    categoryWaitMs: 2500,
    headless: true,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--url" && argv[i + 1]) {
      args.url = argv[i + 1];
      i += 1;
      continue;
    }
    if (token === "--out-dir" && argv[i + 1]) {
      args.outDir = argv[i + 1];
      i += 1;
      continue;
    }
    if (token === "--category-limit" && argv[i + 1]) {
      args.categoryLimit = Number(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--category-wait-ms" && argv[i + 1]) {
      args.categoryWaitMs = Number(argv[i + 1]);
      i += 1;
      continue;
    }
    if (token === "--headed") {
      args.headless = false;
      continue;
    }
  }
  return args;
}

function nowIso() {
  return new Date().toISOString();
}

function toInt(value) {
  if (value == null) return null;
  const digits = String(value).replace(/[^\d]/g, "");
  return digits ? Number(digits) : null;
}

function toCsv(rows, headers) {
  const escape = (value) => {
    if (value == null) return "";
    const s = String(value);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => escape(row[h])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

async function run() {
  const args = parseArgs(process.argv);
  const runId = new Date().toISOString().replace(/[-:.]/g, "").slice(0, 15) + "Z";
  const outDir = path.resolve(process.cwd(), args.outDir, runId);
  fs.mkdirSync(outDir, { recursive: true });

  const browser = await chromium.launch({ headless: args.headless });
  const context = await browser.newContext({
    locale: "ko-KR",
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) " +
      "Chrome/122.0.0.0 Safari/537.36",
    extraHTTPHeaders: {
      "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  });
  const page = await context.newPage();
  await page.goto(args.url, { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.waitForTimeout(4000);

  const categories = await page
    .locator("div.common-menu button[data-ref-dispcatno]")
    .evaluateAll((nodes) =>
      nodes
        .map((node) => {
          const rawCode = (node.getAttribute("data-ref-dispcatno") || "").trim();
          const rawName = (node.textContent || "").trim();
          return {
            rawCode,
            code: rawCode || "ALL",
            name: rawName || "전체",
          };
        })
        .filter((item) => item.name)
    );

  const targetCategories = categories.slice(0, Math.max(0, args.categoryLimit));
  const rows = [];
  const categorySummaries = [];

  for (const category of targetCategories) {
    const buttonCode = category.rawCode || "";
    const locator = page.locator(`button[data-ref-dispcatno="${buttonCode}"]`).first();
    await locator.waitFor({ timeout: 15000 });
    await locator.click({ timeout: 15000 });
    await page.waitForTimeout(args.categoryWaitMs);

    const bodyText = await page.locator("body").innerText().catch(() => "");
    const challengeDetected = /enable javascript and cookies to continue|잠시만 기다려 주세요|접속 정보를 확인 중이에요/i.test(
      bodyText
    );
    if (challengeDetected) {
      categorySummaries.push({
        categoryCode: category.code,
        categoryName: category.name,
        challengeDetected: true,
        goodsCount: 0,
      });
      break;
    }

    const items = await page.locator("div.prd_info").evaluateAll((cards) => {
      const out = [];
      for (const card of cards) {
        const thumb = card.querySelector('a.prd_thumb[href*="getGoodsDetail.do?goodsNo="]');
        if (!thumb) continue;
        const href = thumb.getAttribute("href") || "";
        let goodsNo = null;
        let dispCatNo = null;
        let rank = null;
        try {
          const parsed = new URL(href, "https://www.oliveyoung.co.kr");
          goodsNo = parsed.searchParams.get("goodsNo");
          dispCatNo = parsed.searchParams.get("dispCatNo");
          const rankRaw = parsed.searchParams.get("t_number");
          if (rankRaw && /^\d+$/.test(rankRaw)) rank = Number(rankRaw);
        } catch (_) {
          // ignore malformed URLs
        }

        const brand = (card.querySelector(".tx_brand")?.textContent || "").trim();
        const productName = (card.querySelector(".tx_name")?.textContent || "").trim();
        const originalPriceText = (card.querySelector(".prd_price .tx_org .tx_num")?.textContent || "").trim();
        const discountPriceText = (card.querySelector(".prd_price .tx_cur .tx_num")?.textContent || "").trim();
        const tags = Array.from(card.querySelectorAll(".prd_flag .icon_flag"))
          .map((node) => (node.textContent || "").trim())
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
    });

    const dedup = new Map();
    for (const item of items) {
      if (!item.goodsNo) continue;
      if (!dedup.has(item.goodsNo)) dedup.set(item.goodsNo, item);
    }
    const normalized = Array.from(dedup.values()).sort((a, b) => {
      const ar = Number.isFinite(a.rank) ? a.rank : 9999;
      const br = Number.isFinite(b.rank) ? b.rank : 9999;
      return ar - br;
    });

    for (const item of normalized) {
      let originalPrice = toInt(item.originalPriceText);
      let discountPrice = toInt(item.discountPriceText);
      let discountRate = null;
      const tags = item.tags || [];
      const hasSaleTag = tags.includes("세일");

      if (originalPrice == null && discountPrice != null && !hasSaleTag) {
        originalPrice = discountPrice;
        discountPrice = null;
      } else if (originalPrice != null && discountPrice != null) {
        if (discountPrice < originalPrice) {
          discountRate = Math.round(((originalPrice - discountPrice) * 100) / originalPrice);
        } else {
          discountPrice = null;
          discountRate = null;
        }
      }

      rows.push({
        collected_at_utc: nowIso(),
        category_code: category.code,
        category_name: category.name,
        rank: item.rank,
        goods_no: item.goodsNo,
        detail_disp_cat_no: item.dispCatNo,
        detail_url: item.href ? new URL(item.href, "https://www.oliveyoung.co.kr").toString() : null,
        brand_name: item.brand,
        product_name: item.productName,
        original_price: originalPrice,
        discount_price: discountPrice,
        discount_rate: discountRate,
        ranking_tags: tags.join(","),
      });
    }

    const ranks = normalized.map((item) => item.rank).filter((r) => Number.isFinite(r));
    categorySummaries.push({
      categoryCode: category.code,
      categoryName: category.name,
      challengeDetected: false,
      goodsCount: normalized.length,
      rankMin: ranks.length ? Math.min(...ranks) : null,
      rankMax: ranks.length ? Math.max(...ranks) : null,
      urlAfterClick: page.url(),
    });
  }

  const csvHeaders = [
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
  ];

  const csvPath = path.join(outDir, "ranking_rows.csv");
  const jsonPath = path.join(outDir, "ranking_rows.json");
  const summaryPath = path.join(outDir, "summary.json");
  fs.writeFileSync(csvPath, toCsv(rows, csvHeaders), "utf-8");
  fs.writeFileSync(jsonPath, JSON.stringify(rows, null, 2), "utf-8");
  fs.writeFileSync(
    summaryPath,
    JSON.stringify(
      {
        runId,
        startedAt: nowIso(),
        finishedAt: nowIso(),
        targetUrl: args.url,
        categoriesRequested: targetCategories.length,
        categoriesCaptured: categorySummaries.length,
        rows: rows.length,
        categorySummaries,
        files: { csvPath, jsonPath },
      },
      null,
      2
    ),
    "utf-8"
  );

  await context.close();
  await browser.close();
  console.log(JSON.stringify({ outDir, csvPath, jsonPath, summaryPath, rows: rows.length }, null, 2));
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
