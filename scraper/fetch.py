#!/usr/bin/env python3
"""
Mecklenburg County, NC — Motivated Seller Lead Scraper
Pulls Tax Delinquent records from the county-published PDF advertisement,
parses every row, scores each lead, and writes JSON + CSV outputs.

Sources
-------
- Tax Delinquent PDF : https://mecknc.widen.net/s/tjgf7bcwrj/ind_taxbills_advertisement

Output files
------------
- dashboard/records.json
- data/records.json
- data/ghl_export.csv
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("meck_scraper")

COUNTY = "Mecklenburg County"
STATE  = "NC"

TAX_DEL_PDF_URL = "https://mecknc.widen.net/s/tjgf7bcwrj/ind_taxbills_advertisement"

RETRY_ATTEMPTS = 3
RETRY_DELAY    = 4

ROOT_DIR      = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
DATA_DIR      = ROOT_DIR / "data"

DASHBOARD_JSON = DASHBOARD_DIR / "records.json"
DATA_JSON      = DATA_DIR      / "records.json"
GHL_CSV        = DATA_DIR      / "ghl_export.csv"

CAT_META: dict[str, dict[str, str]] = {
    "TAXDEL":    {"label": "Tax Delinquent",         "flag": "Tax lien"},
    "LP":        {"label": "Lis Pendens",             "flag": "Lis pendens"},
    "NOFC":      {"label": "Notice of Foreclosure",  "flag": "Pre-foreclosure"},
    "TAXDEED":   {"label": "Tax Deed",                "flag": "Tax lien"},
    "JUD":       {"label": "Judgment",                "flag": "Judgment lien"},
    "CCJ":       {"label": "Certified Judgment",      "flag": "Judgment lien"},
    "DRJUD":     {"label": "Domestic Judgment",       "flag": "Judgment lien"},
    "LNCORPTX":  {"label": "Corp Tax Lien",           "flag": "Tax lien"},
    "LNIRS":     {"label": "IRS Lien",                "flag": "Tax lien"},
    "LNFED":     {"label": "Federal Lien",            "flag": "Tax lien"},
    "LN":        {"label": "Lien",                    "flag": "Judgment lien"},
    "LNMECH":    {"label": "Mechanic Lien",           "flag": "Mechanic lien"},
    "LNHOA":     {"label": "HOA Lien",                "flag": "Judgment lien"},
    "MEDLN":     {"label": "Medicaid Lien",           "flag": "Judgment lien"},
    "PRO":       {"label": "Probate Documents",       "flag": "Probate / estate"},
    "NOC":       {"label": "Notice of Commencement",  "flag": "Mechanic lien"},
    "RELLP":     {"label": "Release Lis Pendens",     "flag": "Lis pendens"},
}

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
    "Motivated Seller Flags", "Source", "Public Records URL",
]


# ---------------------------------------------------------------------------
# PDF download — Playwright intercepts the real PDF network request
# ---------------------------------------------------------------------------

async def _playwright_download_pdf(url: str) -> bytes:
    """
    Use Playwright to open the Widen share page in a real browser,
    intercept the PDF network response, and return the raw bytes.

    Widen renders the viewer with JavaScript. When the page loads it
    fires an XHR/fetch for the actual PDF asset. We listen for any
    response whose Content-Type contains 'pdf' OR whose URL ends in
    '.pdf', grab its body, and return it — no fragile URL pattern needed.
    """
    from playwright.async_api import async_playwright

    log.info("Launching Playwright to fetch PDF from: %s", url)
    pdf_bytes: list[bytes] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        # Intercept all responses and capture the first PDF one
        async def handle_response(response):
            if pdf_bytes:
                return  # already captured
            ct = response.headers.get("content-type", "")
            ru = response.url.lower()
            if "pdf" in ct or ru.endswith(".pdf") or "application/pdf" in ct:
                try:
                    body = await response.body()
                    if body[:4] == b"%PDF":
                        log.info("Intercepted PDF response from: %s (%d bytes)",
                                 response.url, len(body))
                        pdf_bytes.append(body)
                except Exception as e:
                    log.warning("Could not read response body: %s", e)

        page.on("response", handle_response)

        # Also set up download interception as fallback
        downloaded: list[bytes] = []
        async def handle_download(download):
            if downloaded:
                return
            path = await download.path()
            if path:
                try:
                    data = Path(path).read_bytes()
                    if data[:4] == b"%PDF":
                        log.info("Captured download: %d bytes", len(data))
                        downloaded.append(data)
                except Exception as e:
                    log.warning("Could not read download: %s", e)

        page.on("download", handle_download)

        try:
            await page.goto(url, timeout=60_000, wait_until="networkidle")
        except Exception as e:
            log.warning("Page load warning (continuing): %s", e)

        # Wait up to 15s for PDF to be intercepted
        for _ in range(30):
            if pdf_bytes or downloaded:
                break
            await asyncio.sleep(0.5)

        # If still no PDF, try clicking a download button
        if not pdf_bytes and not downloaded:
            log.info("No PDF intercepted yet — trying download button click ...")
            for selector in [
                "a[href*='.pdf']",
                "button:has-text('Download')",
                "a:has-text('Download')",
                "[data-testid='download']",
                ".download",
                "#download",
            ]:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        log.info("Clicking: %s", selector)
                        async with page.expect_download(timeout=15_000) as dl_info:
                            await el.click()
                        dl = await dl_info.value
                        path = await dl.path()
                        if path:
                            data = Path(path).read_bytes()
                            if data[:4] == b"%PDF":
                                downloaded.append(data)
                                break
                except Exception as e:
                    log.warning("Click attempt failed (%s): %s", selector, e)

        await browser.close()

    if pdf_bytes:
        return pdf_bytes[0]
    if downloaded:
        return downloaded[0]
    raise RuntimeError("Playwright could not capture the PDF from the Widen page")


def download_pdf(url: str) -> bytes:
    """
    Download the tax delinquent PDF. Tries Playwright first (handles
    JS-rendered Widen viewer), falls back to direct requests GET.
    """
    last_exc: Exception | None = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            log.info("PDF download attempt %d/%d ...", attempt, RETRY_ATTEMPTS)

            # --- Try Playwright (handles JS pages) -----------------------
            try:
                data = asyncio.run(_playwright_download_pdf(url))
                log.info("Playwright download succeeded (%d bytes)", len(data))
                return data
            except Exception as pw_exc:
                log.warning("Playwright attempt failed: %s", pw_exc)

            # --- Fallback: direct requests GET ---------------------------
            log.info("Trying direct requests GET ...")
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                "Accept": "application/pdf,*/*",
            }
            resp = requests.get(url, headers=headers, timeout=60,
                                allow_redirects=True)
            resp.raise_for_status()
            if resp.content[:4] == b"%PDF":
                log.info("Direct GET returned PDF (%d bytes)", len(resp.content))
                return resp.content

            raise RuntimeError(
                f"Direct GET did not return a PDF "
                f"(content-type: {resp.headers.get('content-type', 'unknown')})"
            )

        except Exception as exc:
            last_exc = exc
            log.warning("Attempt %d/%d failed: %s", attempt, RETRY_ATTEMPTS, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)

    raise RuntimeError(
        f"All {RETRY_ATTEMPTS} download attempts failed"
    ) from last_exc


# ---------------------------------------------------------------------------
# PDF parsing — 3-column layout: Name | Situs | Amount
# ---------------------------------------------------------------------------

def _clean(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def _parse_amount(raw: str) -> float:
    if not raw:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


_NC_CITIES = {
    "CHARLOTTE", "HUNTERSVILLE", "CORNELIUS", "DAVIDSON", "PINEVILLE",
    "MATTHEWS", "MINT HILL", "STALLINGS", "INDIAN TRAIL", "MONROE",
    "GASTONIA", "CONCORD", "KANNAPOLIS", "MOORESVILLE", "ROCK HILL",
    "FORT MILL", "BELMONT", "MOUNT HOLLY",
}


def _split_address(raw: str) -> dict[str, str]:
    result = {"street": "", "city": "", "state": STATE, "zip": "", "full": raw}
    if not raw:
        return result
    raw = raw.upper().strip()

    zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\s*$", raw)
    if zip_match:
        result["zip"] = zip_match.group(1)
        raw = raw[: zip_match.start()].strip()

    state_match = re.search(r"\s([A-Z]{2})\s*$", raw)
    if state_match and result["zip"]:
        result["state"] = state_match.group(1)
        raw = raw[: state_match.start()].strip()

    for city in sorted(_NC_CITIES, key=len, reverse=True):
        if raw.endswith(city):
            result["city"] = city.title()
            raw = raw[: -len(city)].strip().rstrip(",")
            break

    result["street"] = raw.title()
    return result


def _owner_flags(name: str) -> list[str]:
    flags: list[str] = []
    upper = name.upper()
    for kw in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "L.L.C", "TRUST"):
        if kw in upper:
            flags.append("LLC / corp owner")
            break
    return flags


def _score_record(record: dict[str, Any]) -> int:
    score = 30
    flags = record.get("flags", [])
    for flag in flags:
        if flag in ("Lis pendens", "Pre-foreclosure", "Judgment lien",
                    "Tax lien", "Mechanic lien", "Probate / estate",
                    "LLC / corp owner", "New this week"):
            score += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amt = record.get("amount", 0.0) or 0.0
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10
    if record.get("prop_address"):
        score += 5
    return min(score, 100)


def parse_tax_delinquent_pdf(pdf_bytes: bytes) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            log.info("PDF has %d page(s)", len(pdf.pages))
            for page_num, page in enumerate(pdf.pages, 1):
                try:
                    _parse_page(page, page_num, records, fetched_at)
                except Exception as exc:
                    log.warning("Page %d parse error (skipping): %s", page_num, exc)
    except Exception as exc:
        log.error("Fatal PDF parse error: %s", exc)
    log.info("Parsed %d tax-delinquent records", len(records))
    return records


def _parse_page(page, page_num, records, fetched_at):
    words = page.extract_words(
        x_tolerance=3, y_tolerance=3,
        keep_blank_chars=False, use_text_flow=False,
    )
    if not words:
        return

    page_w = float(page.width)
    col1_end = page_w * 0.35
    col2_end = page_w * 0.70

    lines: dict[int, list[dict]] = {}
    for w in words:
        y_key = round(float(w["top"]) / 4) * 4
        lines.setdefault(y_key, []).append(w)

    for y_key in sorted(lines.keys()):
        row_words = lines[y_key]
        col1_words, col2_words, col3_words = [], [], []
        for w in row_words:
            x = float(w["x0"])
            if x < col1_end:
                col1_words.append(w["text"])
            elif x < col2_end:
                col2_words.append(w["text"])
            else:
                col3_words.append(w["text"])

        name_raw   = _clean(" ".join(col1_words))
        situs_raw  = _clean(" ".join(col2_words))
        amount_raw = _clean(" ".join(col3_words))

        if not name_raw:
            continue
        low = name_raw.lower()
        if low in ("name", "owner", "taxpayer") or low.startswith("name "):
            continue
        if amount_raw.lower() in ("amount", "tax", "balance"):
            continue
        if len(name_raw) < 3 and not situs_raw:
            continue

        amount_float = _parse_amount(amount_raw)
        addr = _split_address(situs_raw)
        flags: list[str] = ["Tax lien"]
        flags += _owner_flags(name_raw)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        record: dict[str, Any] = {
            "doc_num":      "",
            "doc_type":     "Tax Delinquent Advertisement",
            "filed":        today_str,
            "cat":          "TAXDEL",
            "cat_label":    "Tax Delinquent",
            "owner":        name_raw.title(),
            "grantee":      "",
            "amount":       amount_float,
            "amount_raw":   amount_raw,
            "legal":        "",
            "prop_address": addr["street"],
            "prop_city":    addr["city"],
            "prop_state":   addr["state"],
            "prop_zip":     addr["zip"],
            "prop_full":    addr["full"],
            "mail_address": addr["street"],
            "mail_city":    addr["city"],
            "mail_state":   addr["state"],
            "mail_zip":     addr["zip"],
            "clerk_url":    TAX_DEL_PDF_URL,
            "flags":        list(set(flags)),
            "score":        0,
            "fetched_at":   fetched_at,
            "source":       f"{COUNTY} Tax Delinquent PDF",
            "page":         page_num,
        }
        record["score"] = _score_record(record)
        records.append(record)


# ---------------------------------------------------------------------------
# Build output payload
# ---------------------------------------------------------------------------

def build_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    with_address = sum(1 for r in records if r.get("prop_address"))
    return {
        "fetched_at":   fetched_at,
        "source":       f"{COUNTY} — Tax Delinquent Advertisement PDF",
        "date_range":   datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "total":        len(records),
        "with_address": with_address,
        "county":       COUNTY,
        "state":        STATE,
        "categories": {
            cat: {"label": meta["label"],
                  "count": sum(1 for r in records if r.get("cat") == cat)}
            for cat, meta in CAT_META.items()
        },
        "records": records,
    }


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

def _ensure_dirs() -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def write_json(payload: dict[str, Any]) -> None:
    _ensure_dirs()
    blob = json.dumps(payload, indent=2, default=str)
    for path in (DASHBOARD_JSON, DATA_JSON):
        path.write_text(blob, encoding="utf-8")
        log.info("Wrote %s  (%d records)", path, payload["total"])


def _split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0].title()
    last  = parts[0].title()
    first = " ".join(parts[1:]).title()
    return first, last


def write_ghl_csv(records: list[dict[str, Any]]) -> None:
    _ensure_dirs()
    with open(GHL_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=GHL_COLUMNS)
        writer.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", ""),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", ""),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", ""),
                "Motivated Seller Flags": "|".join(r.get("flags", [])),
                "Source":                 r.get("source", ""),
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("Wrote GHL CSV -> %s  (%d rows)", GHL_CSV, len(records))


# ---------------------------------------------------------------------------
# Dashboard HTML generator
# ---------------------------------------------------------------------------

def write_dashboard_html(payload: dict[str, Any]) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    html_path = DASHBOARD_DIR / "index.html"

    records = payload.get("records", [])
    total   = payload.get("total", 0)
    w_addr  = payload.get("with_address", 0)
    fetched = payload.get("fetched_at", "")

    sorted_recs = sorted(records, key=lambda r: r.get("score", 0), reverse=True)[:500]

    rows_html = ""
    for r in sorted_recs:
        score     = r.get("score", 0)
        score_cls = "score-high" if score >= 70 else ("score-mid" if score >= 50 else "score-low")
        flags_html = " ".join(
            f'<span class="badge">{f}</span>' for f in r.get("flags", [])
        )
        amt = r.get("amount", 0) or 0
        amt_str = f"${amt:,.2f}" if amt else "—"
        addr = ", ".join(filter(None, [
            r.get("prop_address"), r.get("prop_city"),
            r.get("prop_state"), r.get("prop_zip"),
        ]))
        rows_html += f"""
        <tr>
          <td><span class="score-pill {score_cls}">{score}</span></td>
          <td>{r.get('owner','')}</td>
          <td>{addr or '—'}</td>
          <td>{amt_str}</td>
          <td><span class="cat-tag">{r.get('cat_label','')}</span></td>
          <td class="flags-cell">{flags_html}</td>
          <td>{r.get('filed','')}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mecklenburg County — Motivated Seller Leads</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --border: #2d3148;
    --accent: #6c63ff; --green: #22c55e; --yellow: #f59e0b;
    --red: #ef4444; --text: #e2e8f0; --muted: #94a3b8;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; font-size: 14px; }}
  header {{ background: var(--surface); border-bottom: 1px solid var(--border); padding: 18px 32px; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }}
  header h1 {{ font-size: 1.3rem; font-weight: 700; }}
  header .meta {{ color: var(--muted); font-size: 0.82rem; }}
  .stats {{ display: flex; gap: 16px; padding: 20px 32px; flex-wrap: wrap; }}
  .stat-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 24px; min-width: 140px; }}
  .stat-card .val {{ font-size: 2rem; font-weight: 800; color: var(--accent); }}
  .stat-card .lbl {{ color: var(--muted); font-size: 0.78rem; margin-top: 4px; }}
  .toolbar {{ padding: 0 32px 16px; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
  input[type=search] {{ background: var(--surface); border: 1px solid var(--border); color: var(--text); border-radius: 8px; padding: 8px 14px; font-size: 13px; width: 300px; outline: none; }}
  input[type=search]:focus {{ border-color: var(--accent); }}
  select {{ background: var(--surface); border: 1px solid var(--border); color: var(--text); border-radius: 8px; padding: 8px 14px; font-size: 13px; outline: none; }}
  .btn {{ background: var(--accent); color: #fff; border: none; border-radius: 8px; padding: 8px 18px; font-size: 13px; cursor: pointer; font-weight: 600; }}
  .table-wrap {{ padding: 0 32px 40px; overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  thead th {{ background: var(--surface); color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 11px; padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); cursor: pointer; white-space: nowrap; }}
  tbody tr {{ border-bottom: 1px solid var(--border); }}
  tbody tr:hover {{ background: rgba(108,99,255,.08); }}
  td {{ padding: 10px 12px; vertical-align: middle; }}
  .score-pill {{ display: inline-block; width: 36px; text-align: center; border-radius: 6px; padding: 3px 0; font-weight: 700; font-size: 13px; }}
  .score-high {{ background: rgba(34,197,94,.18); color: var(--green); }}
  .score-mid  {{ background: rgba(245,158,11,.18); color: var(--yellow); }}
  .score-low  {{ background: rgba(239,68,68,.18);  color: var(--red); }}
  .badge {{ display: inline-block; background: rgba(108,99,255,.15); color: #a5b4fc; border-radius: 4px; padding: 2px 7px; font-size: 11px; margin: 2px 2px 2px 0; white-space: nowrap; }}
  .cat-tag {{ background: rgba(255,255,255,.06); border-radius: 4px; padding: 3px 8px; font-size: 11px; }}
  .no-data {{ text-align: center; color: var(--muted); padding: 60px 0; }}
</style>
</head>
<body>
<header>
  <div>
    <h1>Mecklenburg County — Motivated Seller Leads</h1>
    <div class="meta">Source: Tax Delinquent Advertisement PDF | Auto-updated daily</div>
  </div>
  <div class="meta">Last fetch: {fetched[:19].replace("T"," ")} UTC</div>
</header>
<div class="stats">
  <div class="stat-card"><div class="val">{total}</div><div class="lbl">Total Leads</div></div>
  <div class="stat-card"><div class="val">{w_addr}</div><div class="lbl">With Address</div></div>
  <div class="stat-card"><div class="val">{sum(1 for r in records if r.get('score',0)>=70)}</div><div class="lbl">High Score (70+)</div></div>
  <div class="stat-card"><div class="val">{sum(1 for r in records if (r.get('amount') or 0)>50000)}</div><div class="lbl">Debt > $50k</div></div>
</div>
<div class="toolbar">
  <input type="search" id="searchBox" placeholder="Search owner, address, city ..." oninput="filterTable()">
  <select id="scoreFilter" onchange="filterTable()">
    <option value="0">All Scores</option>
    <option value="70">High (70+)</option>
    <option value="50">Medium (50+)</option>
  </select>
  <select id="flagFilter" onchange="filterTable()">
    <option value="">All Flags</option>
    <option>Tax lien</option>
    <option>Lis pendens</option>
    <option>Pre-foreclosure</option>
    <option>Judgment lien</option>
    <option>Mechanic lien</option>
    <option>Probate / estate</option>
    <option>LLC / corp owner</option>
  </select>
  <button class="btn" onclick="exportCSV()">Download CSV</button>
  <span id="countLabel" style="color:var(--muted);font-size:12px;"></span>
</div>
<div class="table-wrap">
  <table id="leadsTable">
    <thead>
      <tr>
        <th onclick="sortTable(0)">Score</th>
        <th onclick="sortTable(1)">Owner</th>
        <th onclick="sortTable(2)">Property Address</th>
        <th onclick="sortTable(3)">Amount Owed</th>
        <th>Category</th>
        <th>Flags</th>
        <th onclick="sortTable(6)">Filed</th>
      </tr>
    </thead>
    <tbody id="tableBody">
{rows_html}
    </tbody>
  </table>
  <div id="noData" class="no-data" style="display:none">No records match your filters.</div>
</div>
<script>
const tbody = document.getElementById('tableBody');
const allRows = Array.from(tbody.querySelectorAll('tr'));
let sortDir = {{}};
function filterTable() {{
  const q = document.getElementById('searchBox').value.toLowerCase();
  const minSc = parseInt(document.getElementById('scoreFilter').value) || 0;
  const flag = document.getElementById('flagFilter').value.toLowerCase();
  let vis = 0;
  allRows.forEach(tr => {{
    const text = tr.textContent.toLowerCase();
    const score = parseInt(tr.querySelector('.score-pill')?.textContent || '0');
    const show = (!q || text.includes(q)) && score >= minSc && (!flag || text.includes(flag));
    tr.style.display = show ? '' : 'none';
    if (show) vis++;
  }});
  document.getElementById('countLabel').textContent = vis + ' record(s) shown';
  document.getElementById('noData').style.display = vis === 0 ? '' : 'none';
}}
function sortTable(colIdx) {{
  const asc = !sortDir[colIdx];
  sortDir = {{}};
  sortDir[colIdx] = asc;
  const sorted = allRows.slice().sort((a, b) => {{
    const av = a.cells[colIdx]?.textContent.trim() || '';
    const bv = b.cells[colIdx]?.textContent.trim() || '';
    const an = parseFloat(av.replace(/[^0-9.]/g, ''));
    const bn = parseFloat(bv.replace(/[^0-9.]/g, ''));
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  sorted.forEach(tr => tbody.appendChild(tr));
}}
function exportCSV() {{
  const headers = ['Score','Owner','Property Address','Amount Owed','Category','Flags','Filed'];
  const visRows = allRows.filter(tr => tr.style.display !== 'none');
  const lines = [headers.join(',')];
  visRows.forEach(tr => {{
    const cells = Array.from(tr.cells).map(td => '"' + td.textContent.replace(/"/g,'""').trim() + '"');
    lines.push(cells.join(','));
  }});
  const blob = new Blob([lines.join('\n')], {{type:'text/csv'}});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'mecklenburg_leads.csv';
  a.click();
}}
filterTable();
</script>
</body>
</html>"""

    html_path.write_text(html, encoding="utf-8")
    log.info("Wrote dashboard HTML -> %s", html_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("Mecklenburg County Motivated Seller Scraper — START")
    log.info("=" * 60)

    all_records: list[dict[str, Any]] = []

    try:
        pdf_bytes = download_pdf(TAX_DEL_PDF_URL)
        taxdel    = parse_tax_delinquent_pdf(pdf_bytes)
        all_records.extend(taxdel)
        log.info("Tax Delinquent: %d records ingested", len(taxdel))
    except Exception as exc:
        log.error("Tax Delinquent PDF failed: %s", exc)

    seen: set[tuple] = set()
    deduped: list[dict] = []
    for r in all_records:
        key = (r.get("owner","").upper(), r.get("prop_address","").upper(), r.get("amount",0))
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("After dedup: %d unique records", len(deduped))

    payload = build_payload(deduped)
    write_json(payload)
    write_ghl_csv(deduped)
    write_dashboard_html(payload)

    log.info("=" * 60)
    log.info("DONE — %d total leads  |  %d with address",
             payload["total"], payload["with_address"])
    log.info("=" * 60)


if __name__ == "__main__":
    main()
