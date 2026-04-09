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

    # Full JSON → data/records.json (complete, indented, for exports/integrations)
    full_blob = json.dumps(payload, indent=2, default=str)
    DATA_JSON.write_text(full_blob, encoding="utf-8")
    log.info("Wrote %s  (%d records)", DATA_JSON, payload["total"])

    # Slim JSON → dashboard/records.json (only UI fields, no indentation = ~5x smaller)
    SLIM_FIELDS = ["score","owner","prop_address","prop_city","prop_state","prop_zip",
                   "amount","cat_label","cat","flags","filed","doc_type","doc_num",
                   "mail_address","mail_city","mail_state","mail_zip",
                   "source","clerk_url","phone","email","skiptrace_status"]
    # Sort by score desc, cap at 5000 for dashboard (keeps file under 1MB)
    top_records = sorted(payload.get("records", []),
                         key=lambda r: r.get("score", 0), reverse=True)[:5000]
    slim_records = []
    for r in top_records:
        slim_records.append({
            "score":    r.get("score", 0),
            "owner":    r.get("owner", ""),
            "addr":     r.get("prop_address", ""),
            "city":     r.get("prop_city", ""),
            "state":    r.get("prop_state", ""),
            "zip":      r.get("prop_zip", ""),
            "amount":   r.get("amount", 0) or 0,
            "cat":      r.get("cat_label", ""),
            "cat_code": r.get("cat", ""),
            "flags":    r.get("flags", []),
            "filed":    r.get("filed", ""),
            "doc_type": r.get("doc_type", ""),
            "doc_num":  r.get("doc_num", ""),
            "url":      r.get("clerk_url", ""),
            "phone":    r.get("phone", ""),
            "email":    r.get("email", ""),
            "skiptrace":r.get("skiptrace_status", ""),
        })
    slim_payload = {
        "fetched_at":  payload.get("fetched_at", ""),
        "total":       payload.get("total", 0),
        "with_address":payload.get("with_address", 0),
        "records":     slim_records,
    }
    slim_blob = json.dumps(slim_payload, separators=(',', ':'), default=str)
    DASHBOARD_JSON.write_text(slim_blob, encoding="utf-8")
    log.info("Wrote %s  (%d records, slim format, %d KB)",
             DASHBOARD_JSON, payload["total"], len(slim_blob)//1024)

    # Also write a JS data file — loaded via <script src> which is 100% reliable
    js_data = "window.DASHBOARD_RECORDS=" + slim_blob + ";"
    js_path = DASHBOARD_DIR / "records_data.js"
    js_path.write_text(js_data, encoding="utf-8")
    log.info("Wrote %s  (%d KB)", js_path, len(js_data)//1024)


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
    fetched = payload.get("fetched_at", "")
    html = _build_dashboard_html(fetched)
    html_path.write_text(html, encoding="utf-8")
    log.info("Wrote dashboard HTML -> %s  (%d KB)", html_path, len(html)//1024)


def _build_dashboard_html(fetched):
    """Generate dashboard HTML that fetches records.json at load time."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mecklenburg County — Motivated Seller Intelligence</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#0a0c0f;--panel:#0f1216;--card:#141820;--border:#1e2530;--border2:#252f3d;
  --accent:#00d4aa;--accent2:#0099ff;--warn:#f59e0b;--danger:#ef4444;
  --text:#e8edf5;--text2:#8a96a8;--text3:#4a5568;
  --mono:'IBM Plex Mono',monospace;--sans:'DM Sans',sans-serif;
}}
body{{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;display:flex;height:100vh;overflow:hidden}}
#sidebar{{width:220px;min-width:220px;background:var(--panel);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto;overflow-x:hidden}}
.sidebar-logo{{padding:18px 16px 12px;border-bottom:1px solid var(--border)}}
.sidebar-logo .county{{font-size:10px;color:var(--accent);font-family:var(--mono);letter-spacing:.08em;text-transform:uppercase;margin-bottom:3px}}
.sidebar-logo .title{{font-size:14px;font-weight:600;color:var(--text);line-height:1.3}}
.sidebar-logo .updated{{font-size:10px;color:var(--text3);margin-top:4px;font-family:var(--mono)}}
.sb{{padding:12px 16px 4px}}
.sbl{{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);margin-bottom:7px}}
.filter-group label{{display:flex;align-items:center;gap:7px;cursor:pointer;padding:3px 0;color:var(--text2);font-size:11px;transition:color .15s}}
.filter-group label:hover{{color:var(--text)}}
.filter-group label input[type=checkbox]{{appearance:none;width:13px;height:13px;border:1px solid var(--border2);border-radius:3px;background:transparent;cursor:pointer;position:relative;flex-shrink:0;transition:all .15s}}
.filter-group label input[type=checkbox]:checked{{background:var(--accent);border-color:var(--accent)}}
.filter-group label input[type=checkbox]:checked::after{{content:'';position:absolute;left:3px;top:1px;width:5px;height:7px;border:1.5px solid #000;border-top:none;border-left:none;transform:rotate(45deg)}}
.fc{{margin-left:auto;font-family:var(--mono);font-size:9px;color:var(--text3);background:var(--card);padding:1px 5px;border-radius:3px}}
.score-range label{{font-size:10px;color:var(--text2);margin-bottom:5px;display:flex;justify-content:space-between}}
.score-range input[type=range]{{width:100%;appearance:none;height:3px;background:var(--border2);border-radius:2px;outline:none;margin-top:2px}}
.score-range input[type=range]::-webkit-slider-thumb{{appearance:none;width:13px;height:13px;border-radius:50%;background:var(--accent);cursor:pointer}}
.amt-inputs{{display:flex;gap:5px;margin-top:5px}}
.amt-inputs input{{width:100%;background:var(--card);border:1px solid var(--border2);color:var(--text);border-radius:4px;padding:5px 7px;font-size:10px;font-family:var(--mono);outline:none}}
.amt-inputs input:focus{{border-color:var(--accent)}}
.sb-sources{{padding:12px 16px;border-top:1px solid var(--border);margin-top:auto}}
.src-item{{display:flex;align-items:center;justify-content:space-between;padding:4px 0;font-size:10px;color:var(--text2)}}
.src-badge{{font-size:8px;font-family:var(--mono);padding:1px 5px;border-radius:3px;font-weight:500}}
.bl{{background:rgba(0,212,170,.15);color:var(--accent)}}
.bu{{background:rgba(245,158,11,.15);color:var(--warn)}}
.bs{{background:rgba(74,85,104,.2);color:var(--text3)}}
.reset-btn{{width:100%;margin-top:10px;padding:6px;background:transparent;border:1px solid var(--border2);color:var(--text2);border-radius:5px;cursor:pointer;font-size:10px;transition:all .15s}}
.reset-btn:hover{{border-color:var(--accent);color:var(--accent)}}
#main{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
#topbar{{background:var(--panel);border-bottom:1px solid var(--border);padding:0 18px;display:flex;align-items:center;gap:10px;height:50px;flex-shrink:0}}
#searchBox{{flex:1;max-width:320px;background:var(--card);border:1px solid var(--border2);color:var(--text);border-radius:6px;padding:7px 12px 7px 30px;font-size:12px;outline:none}}
#searchBox:focus{{border-color:var(--accent)}}
.sw{{position:relative}}
.sw::before{{content:'';position:absolute;left:9px;top:50%;transform:translateY(-50%);width:12px;height:12px;border:1.5px solid var(--text3);border-radius:50%;pointer-events:none}}
.sw::after{{content:'';position:absolute;left:18px;top:54%;width:5px;height:1.5px;background:var(--text3);transform:rotate(45deg);pointer-events:none}}
.tb-stats{{display:flex;gap:16px;margin-left:auto;align-items:center}}
.tbs{{text-align:right}}
.tbs .val{{font-family:var(--mono);font-size:17px;font-weight:500;color:var(--text);line-height:1}}
.tbs .val.ac{{color:var(--accent)}}
.tbs .val.wn{{color:var(--warn)}}
.tbs .lbl{{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.07em;margin-top:1px}}
.tb-div{{width:1px;height:28px;background:var(--border)}}
.exp-btn{{background:var(--accent);color:#000;border:none;border-radius:5px;padding:6px 13px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;transition:opacity .15s}}
.exp-btn:hover{{opacity:.85}}
#tabs{{display:flex;padding:0 18px;border-bottom:1px solid var(--border);background:var(--panel);flex-shrink:0}}
.tab{{padding:10px 14px;font-size:11px;color:var(--text2);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;white-space:nowrap}}
.tab:hover{{color:var(--text)}}
.tab.active{{color:var(--accent);border-bottom-color:var(--accent)}}
#tw{{flex:1;overflow:auto}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead{{position:sticky;top:0;z-index:10}}
thead th{{background:var(--card);color:var(--text3);font-weight:500;font-size:10px;text-transform:uppercase;letter-spacing:.07em;padding:9px 13px;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none;font-family:var(--mono)}}
thead th:hover{{color:var(--text2)}}
thead th.sa::after{{content:' ↑';color:var(--accent)}}
thead th.sd::after{{content:' ↓';color:var(--accent)}}
tbody tr{{border-bottom:1px solid var(--border);cursor:pointer;transition:background .1s}}
tbody tr:hover{{background:rgba(0,212,170,.04)}}
tbody tr.sel{{background:rgba(0,212,170,.08)!important}}
td{{padding:8px 13px;vertical-align:middle;color:var(--text2)}}
td.ac{{color:var(--text)}}
td.oc{{color:var(--text);font-weight:500;max-width:190px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.sd2{{display:inline-flex;align-items:center;justify-content:center;width:30px;height:20px;border-radius:3px;font-family:var(--mono);font-size:11px;font-weight:500}}
.sh{{background:rgba(34,197,94,.15);color:#22c55e}}
.sm{{background:rgba(245,158,11,.15);color:#f59e0b}}
.sl{{background:rgba(239,68,68,.12);color:#ef4444}}
.fd{{display:flex;gap:3px;align-items:center}}
.dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
.d0{{background:#ef4444}}.d1{{background:#a78bfa}}.d2{{background:#f59e0b}}.d3{{background:#60a5fa}}
.d4{{background:#34d399}}.d5{{background:#f472b6}}.d6{{background:#94a3b8}}.d7{{background:#00d4aa}}
.chip{{display:inline-block;padding:2px 7px;border-radius:3px;font-size:9px;font-family:var(--mono);background:rgba(0,153,255,.12);color:var(--accent2)}}
.amc{{font-family:var(--mono);font-size:11px;color:var(--text)}}
.amc.big{{color:#f59e0b}}
#nd{{display:none;text-align:center;padding:60px;color:var(--text3);font-size:13px}}
#loading{{text-align:center;padding:60px;color:var(--text3);font-size:13px;font-family:var(--mono)}}
#pb{{background:var(--panel);border-top:1px solid var(--border);padding:7px 18px;display:flex;align-items:center;gap:10px;flex-shrink:0}}
#pb .pc{{font-size:10px;color:var(--text3);font-family:var(--mono)}}
.pctrls{{display:flex;gap:3px;margin-left:auto}}
.pgb{{background:var(--card);border:1px solid var(--border2);color:var(--text2);padding:3px 9px;border-radius:4px;cursor:pointer;font-size:10px;font-family:var(--mono);transition:all .15s}}
.pgb:hover{{border-color:var(--accent);color:var(--accent)}}
.pgb.act{{background:var(--accent);color:#000;border-color:var(--accent)}}
.pgb:disabled{{opacity:.3;cursor:not-allowed}}
.pps{{background:var(--card);border:1px solid var(--border2);color:var(--text2);padding:3px 7px;border-radius:4px;font-size:10px;font-family:var(--mono);outline:none}}
#dp{{width:270px;min-width:270px;background:var(--panel);border-left:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto;transition:width .2s}}
#dp.closed{{width:0;min-width:0;overflow:hidden}}
.dh{{padding:13px 15px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}}
.dh .dt{{font-size:12px;font-weight:600;color:var(--text)}}
.dx{{background:none;border:none;color:var(--text3);cursor:pointer;font-size:15px;line-height:1}}
.dx:hover{{color:var(--text)}}
.dscore{{margin:12px 15px;background:var(--card);border:1px solid var(--border2);border-radius:7px;padding:13px;display:flex;align-items:center;gap:11px}}
.dsn{{font-family:var(--mono);font-size:30px;font-weight:500;line-height:1}}
.dsn.sh{{color:#22c55e}}.dsn.sm{{color:#f59e0b}}.dsn.sl{{color:#ef4444}}
.dsl{{font-size:9px;color:var(--text3);margin-top:2px}}
.dsr{{font-size:10px;color:var(--text2);line-height:1.6}}
.ds{{padding:9px 15px;border-bottom:1px solid var(--border)}}
.dst{{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.1em;font-family:var(--mono);margin-bottom:7px}}
.dr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:5px;gap:7px}}
.dr .dk{{font-size:9px;color:var(--text3);white-space:nowrap}}
.dr .dv{{font-size:10px;color:var(--text);text-align:right;word-break:break-word}}
.dr .dv.mn{{font-family:var(--mono)}}
.dr .dv.big{{color:var(--warn);font-family:var(--mono);font-size:12px;font-weight:500}}
.dfl{{padding:9px 15px;border-bottom:1px solid var(--border)}}
.dfi{{display:flex;align-items:center;gap:6px;padding:3px 0;font-size:10px;color:var(--text2)}}
.dfi::before{{content:'\25c6';font-size:6px;color:var(--accent)}}
.dacts{{padding:11px 15px;display:flex;flex-direction:column;gap:6px}}
.abt{{width:100%;padding:7px;border-radius:5px;font-size:10px;font-weight:500;cursor:pointer;text-align:center;transition:all .15s;border:none}}
.abp{{background:var(--accent);color:#000}}
.abs{{background:transparent;border:1px solid var(--border2)!important;color:var(--text2)}}
.abs:hover{{border-color:var(--accent)!important;color:var(--accent)}}
</style>
</head>
<body>
<nav id="sidebar">
  <div class="sidebar-logo">
    <div class="county">Mecklenburg Co &middot; NC</div>
    <div class="title">Motivated Seller<br>Intelligence</div>
    <div class="updated">Updated: {fetched[:10]}</div>
  </div>
  <div class="sb"><div class="sbl">Categories</div><div class="filter-group">
    <label><input type="checkbox" class="cf" value="TAXDEL" checked> Tax Delinquent <span class="fc" id="c-TAXDEL">0</span></label>
    <label><input type="checkbox" class="cf" value="LP"> Lis Pendens <span class="fc" id="c-LP">0</span></label>
    <label><input type="checkbox" class="cf" value="NOFC"> Foreclosure <span class="fc" id="c-NOFC">0</span></label>
    <label><input type="checkbox" class="cf" value="TAXDEED"> Tax Deed <span class="fc" id="c-TAXDEED">0</span></label>
    <label><input type="checkbox" class="cf" value="JUD"> Judgment <span class="fc" id="c-JUD">0</span></label>
    <label><input type="checkbox" class="cf" value="CCJ"> Cert. Judgment <span class="fc" id="c-CCJ">0</span></label>
    <label><input type="checkbox" class="cf" value="DRJUD"> Dom. Judgment <span class="fc" id="c-DRJUD">0</span></label>
    <label><input type="checkbox" class="cf" value="LNCORPTX"> Corp Tax Lien <span class="fc" id="c-LNCORPTX">0</span></label>
    <label><input type="checkbox" class="cf" value="LNIRS"> IRS Lien <span class="fc" id="c-LNIRS">0</span></label>
    <label><input type="checkbox" class="cf" value="LNFED"> Federal Lien <span class="fc" id="c-LNFED">0</span></label>
    <label><input type="checkbox" class="cf" value="LN"> Lien <span class="fc" id="c-LN">0</span></label>
    <label><input type="checkbox" class="cf" value="LNMECH"> Mechanic Lien <span class="fc" id="c-LNMECH">0</span></label>
    <label><input type="checkbox" class="cf" value="LNHOA"> HOA Lien <span class="fc" id="c-LNHOA">0</span></label>
    <label><input type="checkbox" class="cf" value="MEDLN"> Medicaid Lien <span class="fc" id="c-MEDLN">0</span></label>
    <label><input type="checkbox" class="cf" value="PRO"> Probate <span class="fc" id="c-PRO">0</span></label>
    <label><input type="checkbox" class="cf" value="NOC"> Notice of Comm. <span class="fc" id="c-NOC">0</span></label>
    <label><input type="checkbox" class="cf" value="RELLP"> Release LP <span class="fc" id="c-RELLP">0</span></label>
  </div></div>
  <div class="sb"><div class="sbl">Motivated Seller Flags</div><div class="filter-group">
    <label><input type="checkbox" class="ff" value="Tax lien"> Tax lien</label>
    <label><input type="checkbox" class="ff" value="Lis pendens"> Lis pendens</label>
    <label><input type="checkbox" class="ff" value="Pre-foreclosure"> Pre-foreclosure</label>
    <label><input type="checkbox" class="ff" value="Judgment lien"> Judgment lien</label>
    <label><input type="checkbox" class="ff" value="Mechanic lien"> Mechanic lien</label>
    <label><input type="checkbox" class="ff" value="Probate / estate"> Probate / estate</label>
    <label><input type="checkbox" class="ff" value="LLC / corp owner"> LLC / corp owner</label>
    <label><input type="checkbox" class="ff" value="New this week"> New this week</label>
  </div></div>
  <div class="sb"><div class="sbl">Min Seller Score</div>
    <div class="score-range"><label>Score <span id="sv">0</span>+</label>
    <input type="range" id="sr" min="0" max="100" value="0" oninput="document.getElementById('sv').textContent=this.value;af()"></div>
  </div>
  <div class="sb"><div class="sbl">Amount Due</div>
    <div class="amt-inputs"><input type="number" id="amn" placeholder="Min $" value="2500" oninput="af()"><input type="number" id="amx" placeholder="Max $" oninput="af()"></div>
  </div>
  <div class="sb"><div class="sbl">Skip Trace Status</div><div class="filter-group">
    <label><input type="checkbox" class="skf" value="" checked> All</label>
    <label><input type="checkbox" class="skf" value="complete"> Complete</label>
    <label><input type="checkbox" class="skf" value="pending"> Pending</label>
    <label><input type="checkbox" class="skf" value="none"> Not traced</label>
  </div></div>
  <div class="sb-sources">
    <div class="sbl">Data Sources</div>
    <div class="src-item">Tax Delinquent PDF <span class="src-badge bl">LIVE</span></div>
    <div class="src-item">Public Records <span class="src-badge bs">SOON</span></div>
    <div class="src-item">Foreclosure Map <span class="src-badge bs">SOON</span></div>
    <div class="src-item">Tax File <span class="src-badge bu">UPLOAD</span></div>
    <div class="src-item">Tax Assessor <span class="src-badge bl">LIVE</span></div>
    <button class="reset-btn" onclick="rf()">Reset All Filters</button>
  </div>
</nav>
<div id="main">
  <div id="topbar">
    <div class="sw"><input type="text" id="searchBox" placeholder="Search address, owner, doc type..." oninput="af()"></div>
    <div class="tb-stats">
      <div class="tbs"><div class="val ac" id="ss">...</div><div class="lbl">Showing</div></div>
      <div class="tb-div"></div>
      <div class="tbs"><div class="val ac" id="sh">...</div><div class="lbl">Hot Leads</div></div>
      <div class="tb-div"></div>
      <div class="tbs"><div class="val wn" id="sa">...</div><div class="lbl">Avg Score</div></div>
      <div class="tb-div"></div>
      <div class="tbs"><div class="val" id="sd">...</div><div class="lbl">Total Exposure</div></div>
    </div>
    <button class="exp-btn" onclick="ec()">Export CSV</button>
  </div>
  <div id="tabs">
    <div class="tab active" onclick="st(this,'all')">Live Feed</div>
    <div class="tab" onclick="st(this,'foreclosure')">Foreclosures</div>
    <div class="tab" onclick="st(this,'taxdel')">Tax Delinquent</div>
    <div class="tab" onclick="st(this,'public')">Public Records</div>
    <div class="tab" onclick="st(this,'stack')">Stack</div>
    <div class="tab" onclick="st(this,'deal')">Deal Analyzer</div>
    <div class="tab" onclick="st(this,'export')">Export + Mail</div>
  </div>
  <div id="tw">
    <div id="loading">Loading records...</div>
    <table id="lt" style="display:none">
      <thead><tr>
        <th onclick="sb('addr')" id="h-addr">Address</th>
        <th onclick="sb('owner')" id="h-owner">Owner</th>
        <th onclick="sb('cat_code')" id="h-cat_code">Category</th>
        <th onclick="sb('doc_type')" id="h-doc_type">Doc Type</th>
        <th onclick="sb('filed')" id="h-filed">Filed</th>
        <th onclick="sb('amount')" id="h-amount">Amt Due</th>
        <th onclick="sb('score')" id="h-score">Score</th>
        <th>MS Flags</th>
      </tr></thead>
      <tbody id="tb"></tbody>
    </table>
    <div id="nd">No records match your filters.</div>
  </div>
  <div id="pb">
    <span class="pc" id="pi"></span>
    <select class="pps" id="pp" onchange="gp(1)">
      <option value="50">50 / page</option>
      <option value="100" selected>100 / page</option>
      <option value="250">250 / page</option>
      <option value="500">500 / page</option>
    </select>
    <div class="pctrls" id="pc"></div>
  </div>
</div>
<div id="dp" class="closed">
  <div class="dh"><span class="dt">Property Detail</span><button class="dx" onclick="cd()">&#x2715;</button></div>
  <div id="db"></div>
</div>
<script>
let R=[],fil=[],sk='score',sortAsc=false,cp=1,tab='all';
const FM={{'Tax lien':'d0','Lis pendens':'d1','Pre-foreclosure':'d2','Judgment lien':'d3','Mechanic lien':'d4','Probate / estate':'d5','LLC / corp owner':'d6','New this week':'d7'}};

fetch('./records.json')
  .then(r=>{{if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}})
  .then(data=>{{
    R=data.records||[];
    document.getElementById('loading').style.display='none';
    document.getElementById('lt').style.display='';
    document.getElementById('h-score').classList.add('sd');
    const cc={{}};
    R.forEach(r=>{{cc[r.cat_code]=(cc[r.cat_code]||0)+1}});
    Object.entries(cc).forEach(([k,v])=>{{const e=document.getElementById('c-'+k);if(e)e.textContent=v.toLocaleString()}});
    af();
  }})
  .catch(e=>{{
    document.getElementById('loading').innerHTML='<span style="color:#ef4444">Failed to load data: '+e.message+'</span>';
  }});

function af(){{
  const q=document.getElementById('searchBox').value.toLowerCase();
  const ms=parseInt(document.getElementById('sr').value)||0;
  const an=parseFloat(document.getElementById('amn').value)||0;
  const ax=parseFloat(document.getElementById('amx').value)||Infinity;
  const cats=new Set([...document.querySelectorAll('.cf:checked')].map(e=>e.value));
  const flags=[...document.querySelectorAll('.ff:checked')].map(e=>e.value).filter(Boolean);
  fil=R.filter(r=>{{
    if(!cats.has(r.cat_code))return false;
    if(r.score<ms)return false;
    if((r.amount||0)<an||(r.amount||0)>ax)return false;
    if(flags.length&&!flags.some(f=>(r.flags||[]).includes(f)))return false;
    if(q){{const h=((r.addr||'')+' '+(r.city||'')+' '+(r.owner||'')+' '+(r.cat||'')+' '+(r.doc_type||'')).toLowerCase();if(!h.includes(q))return false}}
    if(tab==='foreclosure'&&r.cat_code!=='NOFC')return false;
    if(tab==='taxdel'&&r.cat_code!=='TAXDEL')return false;
    return true;
  }});
  srt();cp=1;rn();us();
}}
function srt(){{
  fil.sort((a,b)=>{{
    let av=a[sk]??'',bv=b[sk]??'';
    if(typeof av==='string')av=av.toLowerCase();
    if(typeof bv==='string')bv=bv.toLowerCase();
    if(av<bv)return sortAsc?-1:1;if(av>bv)return sortAsc?1:-1;return 0;
  }});
}}
function sb(k){{
  if(sk===k)sortAsc=!sortAsc;else{{sk=k;sortAsc=false}}
  document.querySelectorAll('thead th').forEach(t=>t.classList.remove('sa','sd'));
  const h=document.getElementById('h-'+k);if(h)h.classList.add(sortAsc?'sa':'sd');
  srt();rn();
}}
function rn(){{
  const pp=parseInt(document.getElementById('pp').value)||100;
  const st=(cp-1)*pp;
  const pg=fil.slice(st,st+pp);
  const tbody=document.getElementById('tb');
  const rows=[];
  for(let i=0;i<pg.length;i++){{
    const r=pg[i],gi=st+i;
    const sc=r.score>=70?'sh':r.score>=50?'sm':'sl';
    const addr=[(r.addr||''),(r.city||''),(r.state||''),(r.zip||'')].filter(Boolean).join(', ')||'&mdash;';
    const amt=(r.amount||0)>0?'$'+(r.amount).toLocaleString('en-US',{{minimumFractionDigits:2,maximumFractionDigits:2}}):'&mdash;';
    const ac=(r.amount||0)>50000?'amc big':'amc';
    const dots=(r.flags||[]).map(f=>'<span class="dot '+(FM[f]||'')+'" title="'+f+'"></span>').join('');
    rows.push('<tr onclick="sr2('+gi+')" data-i="'+gi+'"><td class="ac">'+addr+'</td><td class="oc">'+(r.owner||'&mdash;')+'</td><td><span class="chip">'+(r.cat_code||'')+'</span></td><td style="color:var(--text2)">'+(r.doc_type||'&mdash;')+'</td><td style="font-family:var(--mono);color:var(--text3)">'+(r.filed||'&mdash;')+'</td><td class="'+ac+'">'+amt+'</td><td><span class="sd2 '+sc+'">'+r.score+'</span></td><td><div class="fd">'+dots+'</div></td></tr>');
  }}
  tbody.innerHTML=rows.join('');
  document.getElementById('nd').style.display=fil.length===0?'block':'none';
  rp(pp);
}}
function rp(pp){{
  const tot=fil.length,pages=Math.max(1,Math.ceil(tot/pp));
  const st=(cp-1)*pp+1,en=Math.min(cp*pp,tot);
  document.getElementById('pi').textContent=(tot>0?st.toLocaleString()+'–'+en.toLocaleString():0)+' of '+tot.toLocaleString();
  const ctrl=document.getElementById('pc');
  let h='<button class="pgb" onclick="gp('+(cp-1)+')" '+(cp<=1?'disabled':'')+'>&#8249; Prev</button>';
  let sp=Math.max(1,cp-2),ep=Math.min(pages,sp+4);
  if(ep-sp<4)sp=Math.max(1,ep-4);
  if(sp>1)h+='<button class="pgb" onclick="gp(1)">1</button><span style="color:var(--text3);padding:0 3px">&hellip;</span>';
  for(let p=sp;p<=ep;p++)h+='<button class="pgb '+(p===cp?'act':'')+'" onclick="gp('+p+')">'+p+'</button>';
  if(ep<pages)h+='<span style="color:var(--text3);padding:0 3px">&hellip;</span><button class="pgb" onclick="gp('+pages+')">'+pages+'</button>';
  h+='<button class="pgb" onclick="gp('+(cp+1)+')" '+(cp>=pages?'disabled':'')+'>Next &#8250;</button>';
  ctrl.innerHTML=h;
}}
function gp(p){{const pp=parseInt(document.getElementById('pp').value)||100;const pages=Math.max(1,Math.ceil(fil.length/pp));cp=Math.max(1,Math.min(p,pages));rn();document.getElementById('tw').scrollTop=0;}}
function us(){{
  const n=fil.length,hot=fil.filter(r=>r.score>=70).length;
  const avg=n?Math.round(fil.reduce((s,r)=>s+r.score,0)/n):0;
  const debt=fil.reduce((s,r)=>s+(r.amount||0),0);
  document.getElementById('ss').textContent=n.toLocaleString();
  document.getElementById('sh').textContent=hot.toLocaleString();
  document.getElementById('sa').textContent=avg;
  document.getElementById('sd').textContent='$'+debt.toLocaleString('en-US',{{maximumFractionDigits:0}});
}}
function sr2(idx){{
  document.querySelectorAll('#tb tr').forEach(t=>t.classList.remove('sel'));
  const tr=document.querySelector('#tb tr[data-i="'+idx+'"]');
  if(tr)tr.classList.add('sel');
  const r=fil[idx];if(!r)return;
  const sc=r.score>=70?'sh':r.score>=50?'sm':'sl';
  const lbl=r.score>=70?'High motivation':r.score>=50?'Medium motivation':'Low motivation';
  const addr=[(r.addr||''),(r.city||''),(r.state||''),(r.zip||'')].filter(Boolean).join(', ')||'&mdash;';
  const amt=(r.amount||0)?'$'+(r.amount).toLocaleString('en-US',{{minimumFractionDigits:2}}):'&mdash;';
  const fi=(r.flags||[]).map(f=>'<div class="dfi">'+f+'</div>').join('');
  document.getElementById('db').innerHTML='<div class="dscore"><div><div class="dsn '+sc+'">'+r.score+'</div><div class="dsl">Seller score</div></div><div class="dsr">'+lbl+'<br><span style="color:var(--text3)">'+(r.flags||[]).length+' signal'+((r.flags||[]).length!==1?'s':'')+'</span></div></div><div class="ds"><div class="dst">Property</div><div class="dr"><span class="dk">Address</span><span class="dv">'+addr+'</span></div><div class="dr"><span class="dk">City / Zip</span><span class="dv">'+(r.city||'&mdash;')+' '+(r.zip||'')+'</span></div></div><div class="ds"><div class="dst">Owner</div><div class="dr"><span class="dk">Name</span><span class="dv">'+(r.owner||'&mdash;')+'</span></div><div class="dr"><span class="dk">Phone</span><span class="dv mn">'+(r.phone||'&mdash;')+'</span></div><div class="dr"><span class="dk">Email</span><span class="dv">'+(r.email||'&mdash;')+'</span></div><div class="dr"><span class="dk">Skip Trace</span><span class="dv">'+(r.skiptrace||'Not run')+'</span></div></div><div class="ds"><div class="dst">Filing</div><div class="dr"><span class="dk">Doc #</span><span class="dv mn">'+(r.doc_num||'&mdash;')+'</span></div><div class="dr"><span class="dk">Category</span><span class="dv">'+(r.cat_code||'')+'</span></div><div class="dr"><span class="dk">Filed</span><span class="dv mn">'+(r.filed||'&mdash;')+'</span></div><div class="dr"><span class="dk">Amt Due</span><span class="dv big">'+amt+'</span></div></div>'+(fi?'<div class="dfl"><div class="dst">Motivated Seller Signals</div>'+fi+'</div>':'')+'<div class="dacts">'+(r.url?'<a href="'+r.url+'" target="_blank" style="text-decoration:none"><button class="abt abs">View Public Record &#x2197;</button></a>':'')+'<button class="abt abs" onclick="alert(\'Skip trace coming soon\')">Run Skip Trace</button><button class="abt abs" onclick="alert(\'Deal analyzer coming soon\')">Run Deal Analysis</button><button class="abt abs" onclick="alert(\'Stack coming soon\')">Add to Stack</button></div>';
  document.getElementById('dp').classList.remove('closed');
}}
function cd(){{document.getElementById('dp').classList.add('closed');document.querySelectorAll('#tb tr').forEach(t=>t.classList.remove('sel'));}}
function st(el,t){{document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));el.classList.add('active');tab=t;af();}}
function rf(){{
  document.querySelectorAll('.cf').forEach(e=>e.checked=true);
  document.querySelectorAll('.ff').forEach(e=>e.checked=false);
  document.getElementById('sr').value=0;document.getElementById('sv').textContent='0';
  document.getElementById('amn').value='';document.getElementById('amx').value='';
  document.getElementById('searchBox').value='';af();
}}
function ec(){{
  const h=['Score','Owner','Property Address','City','State','Zip','Amount Due','Category','Doc Type','Filed','Flags','Phone','Email','Skip Trace','Source','URL'];
  const lines=[h.join(',')];
  fil.forEach(r=>lines.push([r.score,'"'+(r.owner||'').replace(/"/g,'""')+'"','"'+(r.addr||'').replace(/"/g,'""')+'"',(r.city||''),(r.state||''),(r.zip||''),(r.amount||0),'"'+(r.cat||'').replace(/"/g,'""')+'"','"'+(r.doc_type||'').replace(/"/g,'""')+'"',(r.filed||''),'"'+((r.flags||[]).join('|'))+'"',(r.phone||''),(r.email||''),(r.skiptrace||''),(r.source||''),(r.url||'')].join(',')));
  const b=new Blob([lines.join('\n')],{{type:'text/csv'}});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='mecklenburg_leads.csv';a.click();
}}
</script>
</body>
</html>"""


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
