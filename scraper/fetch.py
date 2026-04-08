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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("meck_scraper")

# ---------------------------------------------------------------------------
# Constants / config
# ---------------------------------------------------------------------------
COUNTY = "Mecklenburg County"
STATE  = "NC"

TAX_DEL_PDF_URL = (
    "https://mecknc.widen.net/s/tjgf7bcwrj/ind_taxbills_advertisement"
)

RETRY_ATTEMPTS = 3
RETRY_DELAY    = 4   # seconds between retries

# Where this script lives → project root is one level up
ROOT_DIR      = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
DATA_DIR      = ROOT_DIR / "data"

DASHBOARD_JSON = DASHBOARD_DIR / "records.json"
DATA_JSON      = DATA_DIR      / "records.json"
GHL_CSV        = DATA_DIR      / "ghl_export.csv"

# ---------------------------------------------------------------------------
# Category / document-type metadata
# ---------------------------------------------------------------------------
CAT_META: dict[str, dict[str, str]] = {
    "TAXDEL":    {"label": "Tax Delinquent",          "flag": "Tax lien"},
    "LP":        {"label": "Lis Pendens",              "flag": "Lis pendens"},
    "NOFC":      {"label": "Notice of Foreclosure",   "flag": "Pre-foreclosure"},
    "TAXDEED":   {"label": "Tax Deed",                 "flag": "Tax lien"},
    "JUD":       {"label": "Judgment",                 "flag": "Judgment lien"},
    "CCJ":       {"label": "Certified Judgment",       "flag": "Judgment lien"},
    "DRJUD":     {"label": "Domestic Judgment",        "flag": "Judgment lien"},
    "LNCORPTX":  {"label": "Corp Tax Lien",            "flag": "Tax lien"},
    "LNIRS":     {"label": "IRS Lien",                 "flag": "Tax lien"},
    "LNFED":     {"label": "Federal Lien",             "flag": "Tax lien"},
    "LN":        {"label": "Lien",                     "flag": "Judgment lien"},
    "LNMECH":    {"label": "Mechanic Lien",            "flag": "Mechanic lien"},
    "LNHOA":     {"label": "HOA Lien",                 "flag": "Judgment lien"},
    "MEDLN":     {"label": "Medicaid Lien",            "flag": "Judgment lien"},
    "PRO":       {"label": "Probate Documents",        "flag": "Probate / estate"},
    "NOC":       {"label": "Notice of Commencement",   "flag": "Mechanic lien"},
    "RELLP":     {"label": "Release Lis Pendens",      "flag": "Lis pendens"},
}

GHL_COLUMNS = [
    "First Name",
    "Last Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Property Address",
    "Property City",
    "Property State",
    "Property Zip",
    "Lead Type",
    "Document Type",
    "Date Filed",
    "Document Number",
    "Amount/Debt Owed",
    "Seller Score",
    "Motivated Seller Flags",
    "Source",
    "Public Records URL",
]

# ---------------------------------------------------------------------------
# Helpers: HTTP with retry
# ---------------------------------------------------------------------------

def _get(url: str, *, stream: bool = False, timeout: int = 30) -> requests.Response:
    """GET with retry logic (3 attempts, exponential back-off)."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url, headers=headers, stream=stream,
                timeout=timeout, allow_redirects=True
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            log.warning("GET %s  attempt %d/%d failed: %s", url, attempt, RETRY_ATTEMPTS, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError(f"Failed to GET {url} after {RETRY_ATTEMPTS} attempts") from last_exc


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def download_pdf(url: str) -> bytes:
    """Download a PDF and return its raw bytes."""
    log.info("Downloading Tax Delinquent PDF …")
    resp = _get(url, stream=True, timeout=60)
    content = resp.content
    log.info("Downloaded %d bytes", len(content))
    return content


# ---------------------------------------------------------------------------
# PDF parsing — 3-column layout: Name | Situs | Amount
# ---------------------------------------------------------------------------

def _clean(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def _parse_amount(raw: str) -> float:
    """Convert '$1,234.56' → 1234.56; return 0.0 on failure."""
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
    """
    Best-effort split of a full address string into components.

    Expected formats (from Mecklenburg tax bills):
        123 MAIN ST CHARLOTTE NC 28202
        456 OAK AVE APT 2B CHARLOTTE NC 28205-1234
    """
    result = {
        "street": "",
        "city": "",
        "state": STATE,
        "zip": "",
        "full": raw,
    }
    if not raw:
        return result

    raw = raw.upper().strip()

    # 1. Try to pull zip (5 or 9 digit)
    zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\s*$", raw)
    if zip_match:
        result["zip"] = zip_match.group(1)
        raw = raw[: zip_match.start()].strip()

    # 2. Try to pull state abbreviation (2 uppercase letters at end)
    # Only strip it if we already found a zip — avoids "101 FIRST ST" → state=ST
    state_match = re.search(r"\s([A-Z]{2})\s*$", raw)
    if state_match and result["zip"]:
        result["state"] = state_match.group(1)
        raw = raw[: state_match.start()].strip()

    # 3. Try known Mecklenburg-area city names at end
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
    score = 30  # base

    flags = record.get("flags", [])
    for flag in flags:
        if flag in ("Lis pendens", "Pre-foreclosure",
                    "Judgment lien", "Tax lien",
                    "Mechanic lien", "Probate / estate",
                    "LLC / corp owner", "New this week"):
            score += 10

    # LP + foreclosure combo bonus
    has_lp = "Lis pendens" in flags
    has_fc = "Pre-foreclosure" in flags
    if has_lp and has_fc:
        score += 20

    # Amount bonuses
    amt = record.get("amount", 0.0) or 0.0
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10

    # Has a property address
    if record.get("prop_address"):
        score += 5

    # "New this week" already counted in flags loop above
    return min(score, 100)


def parse_tax_delinquent_pdf(pdf_bytes: bytes) -> list[dict[str, Any]]:
    """
    Parse the Mecklenburg County tax-delinquent advertisement PDF.

    The PDF is laid out in **3 columns** across the page:
        Column 1: Owner Name
        Column 2: Situs (Property Address)
        Column 3: Amount Owed

    Each page typically has a header row ("NAME / SITUS / AMOUNT") plus
    data rows. We use pdfplumber's word-level bounding boxes to
    reconstruct columns by x-coordinate.
    """
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


def _parse_page(
    page,
    page_num: int,
    records: list[dict[str, Any]],
    fetched_at: str,
) -> None:
    """
    Extract rows from a single page using word-level x-positions to
    bucket words into three columns.
    """
    words = page.extract_words(
        x_tolerance=3,
        y_tolerance=3,
        keep_blank_chars=False,
        use_text_flow=False,
    )
    if not words:
        return

    # Determine page width and approximate column boundaries.
    # The PDF typically uses equal thirds.
    page_w = float(page.width)
    col1_end = page_w * 0.35
    col2_end = page_w * 0.70

    # Group words by line (y0 rounded to nearest 4 pts)
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

        # Skip header rows / empty rows
        if not name_raw:
            continue
        low = name_raw.lower()
        if low in ("name", "owner", "taxpayer") or low.startswith("name "):
            continue
        if amount_raw.lower() in ("amount", "tax", "balance"):
            continue

        # Skip pure-number or very short junk rows
        if len(name_raw) < 3 and not situs_raw:
            continue

        amount_float = _parse_amount(amount_raw)

        addr = _split_address(situs_raw)

        flags: list[str] = ["Tax lien"]   # always for TAXDEL
        flags += _owner_flags(name_raw)

        # We don't have a filed date in the PDF — use today
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
            "mail_address": addr["street"],   # use prop address as mail default
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
        "fetched_at":    fetched_at,
        "source":        f"{COUNTY} — Tax Delinquent Advertisement PDF",
        "date_range":    datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "total":         len(records),
        "with_address":  with_address,
        "county":        COUNTY,
        "state":         STATE,
        "categories": {
            cat: {"label": meta["label"], "count": sum(1 for r in records if r.get("cat") == cat)}
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
    """Split 'SMITH JOHN A' → ('John', 'Smith') (last name first in tax rolls)."""
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
    log.info("Wrote GHL CSV → %s  (%d rows)", GHL_CSV, len(records))


# ---------------------------------------------------------------------------
# Async Playwright helper — reserved for future clerk-portal scraping
# ---------------------------------------------------------------------------

async def _playwright_fetch(url: str) -> str:
    """
    Fetch a JavaScript-rendered page via Playwright (Chromium).
    Currently reserved for Mecklenburg Register of Deeds portal.
    Returns the full page HTML as a string.
    """
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except ImportError:
        log.warning("Playwright not installed — skipping JS-rendered fetch")
        return ""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()
        try:
            await page.goto(url, timeout=45_000, wait_until="networkidle")
            content = await page.content()
        finally:
            await browser.close()
    return content


# ---------------------------------------------------------------------------
# Dashboard index.html generator
# ---------------------------------------------------------------------------

def write_dashboard_html(payload: dict[str, Any]) -> None:
    """Generate a self-contained dashboard/index.html from records.json."""
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    html_path = DASHBOARD_DIR / "index.html"

    records = payload.get("records", [])
    total   = payload.get("total", 0)
    w_addr  = payload.get("with_address", 0)
    fetched = payload.get("fetched_at", "")

    # Build table rows (top 500 by score)
    sorted_recs = sorted(records, key=lambda r: r.get("score", 0), reverse=True)[:500]

    rows_html = ""
    for r in sorted_recs:
        score      = r.get("score", 0)
        score_cls  = "score-high" if score >= 70 else ("score-mid" if score >= 50 else "score-low")
        flags_html = " ".join(
            f'<span class="badge">{f}</span>'
            for f in r.get("flags", [])
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
    --bg: #0f1117;
    --surface: #1a1d27;
    --border: #2d3148;
    --accent: #6c63ff;
    --green: #22c55e;
    --yellow: #f59e0b;
    --red: #ef4444;
    --text: #e2e8f0;
    --muted: #94a3b8;
    --font: 'Inter', system-ui, sans-serif;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; }}

  header {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 18px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
  }}
  header h1 {{ font-size: 1.3rem; font-weight: 700; }}
  header .meta {{ color: var(--muted); font-size: 0.82rem; }}

  .stats {{
    display: flex;
    gap: 16px;
    padding: 20px 32px;
    flex-wrap: wrap;
  }}
  .stat-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 24px;
    min-width: 140px;
  }}
  .stat-card .val {{ font-size: 2rem; font-weight: 800; color: var(--accent); }}
  .stat-card .lbl {{ color: var(--muted); font-size: 0.78rem; margin-top: 4px; }}

  .toolbar {{
    padding: 0 32px 16px;
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
  }}
  input[type=search] {{
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    border-radius: 8px;
    padding: 8px 14px;
    font-size: 13px;
    width: 300px;
    outline: none;
  }}
  input[type=search]:focus {{ border-color: var(--accent); }}
  select {{
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    border-radius: 8px;
    padding: 8px 14px;
    font-size: 13px;
    outline: none;
  }}
  .btn {{
    background: var(--accent);
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 8px 18px;
    font-size: 13px;
    cursor: pointer;
    font-weight: 600;
  }}
  .btn:hover {{ opacity: .88; }}

  .table-wrap {{
    padding: 0 32px 40px;
    overflow-x: auto;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  thead th {{
    background: var(--surface);
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: .05em;
    padding: 10px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    cursor: pointer;
    user-select: none;
  }}
  thead th:hover {{ color: var(--text); }}
  tbody tr {{ border-bottom: 1px solid var(--border); transition: background .1s; }}
  tbody tr:hover {{ background: rgba(108,99,255,.08); }}
  td {{ padding: 10px 12px; vertical-align: middle; }}

  .score-pill {{
    display: inline-block;
    width: 36px;
    text-align: center;
    border-radius: 6px;
    padding: 3px 0;
    font-weight: 700;
    font-size: 13px;
  }}
  .score-high {{ background: rgba(34,197,94,.18); color: var(--green); }}
  .score-mid  {{ background: rgba(245,158,11,.18); color: var(--yellow); }}
  .score-low  {{ background: rgba(239,68,68,.18);  color: var(--red); }}

  .badge {{
    display: inline-block;
    background: rgba(108,99,255,.15);
    color: #a5b4fc;
    border-radius: 4px;
    padding: 2px 7px;
    font-size: 11px;
    margin: 2px 2px 2px 0;
    white-space: nowrap;
  }}
  .cat-tag {{
    background: rgba(255,255,255,.06);
    border-radius: 4px;
    padding: 3px 8px;
    font-size: 11px;
    white-space: nowrap;
  }}
  .flags-cell {{ max-width: 280px; }}

  .no-data {{ text-align: center; color: var(--muted); padding: 60px 0; }}

  @media (max-width: 700px) {{
    .stats, .toolbar, .table-wrap, header {{ padding-left: 16px; padding-right: 16px; }}
    input[type=search] {{ width: 100%; }}
  }}
</style>
</head>
<body>

<header>
  <div>
    <h1>🏠 Mecklenburg County — Motivated Seller Leads</h1>
    <div class="meta">Source: Tax Delinquent Advertisement PDF &nbsp;|&nbsp; Auto-updated daily via GitHub Actions</div>
  </div>
  <div class="meta">Last fetch: {fetched[:19].replace("T"," ")} UTC</div>
</header>

<div class="stats">
  <div class="stat-card"><div class="val">{total}</div><div class="lbl">Total Leads</div></div>
  <div class="stat-card"><div class="val">{w_addr}</div><div class="lbl">With Address</div></div>
  <div class="stat-card"><div class="val">{sum(1 for r in records if r.get('score',0)>=70)}</div><div class="lbl">High Score (&ge;70)</div></div>
  <div class="stat-card"><div class="val">{sum(1 for r in records if (r.get('amount') or 0)>50000)}</div><div class="lbl">Debt &gt; $50k</div></div>
</div>

<div class="toolbar">
  <input type="search" id="searchBox" placeholder="Search owner, address, city …" oninput="filterTable()">
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
    <option>New this week</option>
  </select>
  <button class="btn" onclick="exportCSV()">⬇ Export CSV</button>
  <span id="countLabel" style="color:var(--muted);font-size:12px;"></span>
</div>

<div class="table-wrap">
  <table id="leadsTable">
    <thead>
      <tr>
        <th onclick="sortTable(0)">Score ↕</th>
        <th onclick="sortTable(1)">Owner</th>
        <th onclick="sortTable(2)">Property Address</th>
        <th onclick="sortTable(3)">Amount Owed ↕</th>
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
  const q     = document.getElementById('searchBox').value.toLowerCase();
  const minSc = parseInt(document.getElementById('scoreFilter').value) || 0;
  const flag  = document.getElementById('flagFilter').value.toLowerCase();
  let vis = 0;
  allRows.forEach(tr => {{
    const text  = tr.textContent.toLowerCase();
    const score = parseInt(tr.querySelector('.score-pill')?.textContent || '0');
    const show  = (!q || text.includes(q))
               && score >= minSc
               && (!flag || text.includes(flag));
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
    const an = parseFloat(av.replace(/[^\\d.]/g, ''));
    const bn = parseFloat(bv.replace(/[^\\d.]/g, ''));
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  sorted.forEach(tr => tbody.appendChild(tr));
}}

function exportCSV() {{
  const headers = ['Score','Owner','Property Address','Amount Owed','Category','Flags','Filed'];
  const visRows = allRows.filter(tr => tr.style.display !== 'none');
  const lines   = [headers.join(',')];
  visRows.forEach(tr => {{
    const cells = Array.from(tr.cells).map(td => '"' + td.textContent.replace(/"/g,'""').trim() + '"');
    lines.push(cells.join(','));
  }});
  const blob = new Blob([lines.join('\\n')], {{type:'text/csv'}});
  const a    = document.createElement('a');
  a.href     = URL.createObjectURL(blob);
  a.download = 'mecklenburg_leads.csv';
  a.click();
}}

// Initial count
filterTable();
</script>
</body>
</html>"""

    html_path.write_text(html, encoding="utf-8")
    log.info("Wrote dashboard HTML → %s", html_path)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("Mecklenburg County Motivated Seller Scraper — START")
    log.info("=" * 60)

    all_records: list[dict[str, Any]] = []

    # ── 1. Tax Delinquent PDF ──────────────────────────────────────────────
    try:
        pdf_bytes = download_pdf(TAX_DEL_PDF_URL)
        taxdel    = parse_tax_delinquent_pdf(pdf_bytes)
        all_records.extend(taxdel)
        log.info("Tax Delinquent: %d records ingested", len(taxdel))
    except Exception as exc:
        log.error("Tax Delinquent PDF failed: %s", exc)

    # ── 2. Future sources (stubs — wire up when available) ─────────────────
    # e.g.:
    #   lis_pendens = await fetch_register_of_deeds("LP")
    #   all_records.extend(lis_pendens)

    # ── 3. Deduplicate by (owner, prop_address, amount) ───────────────────
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for r in all_records:
        key = (r.get("owner","").upper(), r.get("prop_address","").upper(), r.get("amount",0))
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("After dedup: %d unique records", len(deduped))

    # ── 4. Build payload & write outputs ──────────────────────────────────
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
