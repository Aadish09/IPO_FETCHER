#!/usr/bin/env python3
"""
Zero-DB IPO Agent with Finnhub integration.

Usage:
1. pip install requests pdfplumber
2. export TELEGRAM_BOT_TOKEN="12345:ABC..."
   export TELEGRAM_CHAT_ID="987654321"
   export FINNHUB_API_KEY="d4cp5uhr01qudf6jdbj0d4cp5uhr01qudf6jdbjg"
   export POLL_INTERVAL_SECONDS=300   # optional
3. python zero_db_ipo_agent.py
"""

import os
import json
import time
import math
import statistics
import requests
from decimal import Decimal
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# ----- Config -----
POLL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # default 5 minutes
DATA_DIR = os.path.join(os.getcwd(), "data")
IPOS_FILE = os.path.join(DATA_DIR, "ipos.json")
GMP_FILE = os.path.join(DATA_DIR, "gmp.json")
FUND_FILE = os.path.join(DATA_DIR, "fundamentals.json")
FINNHUB_RAW_FILE = os.path.join(DATA_DIR, "finnhub_raw.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# GMP sources (placeholder). Replace with real URLs/APIs.
GMP_SOURCES = [
    # Add real GMP page URLs here
    # "https://example-gmp-source-1.com/ipo/gmp/example-corp",
    # "https://example-gmp-source-2.com/gmp/example-corp"
]

# Simple threshold for demo notify (INR)
GMP_NOTIFY_THRESHOLD = 50.0

# Finnhub settings
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_MAX_RETRIES = 5
FINNHUB_RETRY_BACKOFF = 2.0  # exponential base seconds

# ----- Utilities: file-based state -----
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_json(path: str, obj: Dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)

# ----- In-memory file-backed state -----
ensure_data_dir()
ipos_state = load_json(IPOS_FILE)        # previous/known IPOs
gmp_state = load_json(GMP_FILE) or {}    # historical gmp readings
funds_state = load_json(FUND_FILE) or {} # stored fundamentals
finnhub_raw_state = load_json(FINNHUB_RAW_FILE) or {}

# ----- Finnhub ingestion -----
def finnhub_get(endpoint: str, params: dict, max_retries: int = FINNHUB_MAX_RETRIES) -> dict:
    """
    Simple GET with retry/backoff for Finnhub API.
    Retries on 429 and transient 5xx errors.
    """
    headers = {"Accept": "application/json"}
    params = dict(params)
    params["token"] = FINNHUB_API_KEY
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{FINNHUB_BASE}{endpoint}", params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                # rate limited — backoff and retry
                print(f"Finnhub 429 rate limit; backing off {backoff}s (attempt {attempt+1}/{max_retries})")
                time.sleep(backoff)
                backoff *= FINNHUB_RETRY_BACKOFF
                continue
            if 500 <= r.status_code < 600:
                print(f"Finnhub server error {r.status_code}; retrying in {backoff}s")
                time.sleep(backoff)
                backoff *= FINNHUB_RETRY_BACKOFF
                continue
            # other errors: raise
            r.raise_for_status()
        except requests.RequestException as e:
            print("Finnhub request error:", str(e), f"(attempt {attempt+1}/{max_retries})")
            time.sleep(backoff)
            backoff *= FINNHUB_RETRY_BACKOFF
            continue
    # if we get here, give up gracefully
    print("Finnhub: exceeded retries, returning empty dict")
    return {}

def map_finnhub_item_to_ipo(it: dict) -> dict:
    """
    Map a single Finnhub item to internal IPO dict format.
    Keep raw item for provenance.
    """
    # Defensive extraction — Finnhub's keys may differ; save raw
    company = it.get("name") or it.get("company") or it.get("description") or it.get("title")
    # Some responses may include 'symbol' or 'ticker'
    symbol = it.get("symbol") or it.get("ticker")
    exchange = it.get("exchange") or it.get("market")
    # Finnhub often uses date fields like 'date' or 'expectedDate' — check multiple
    open_date = it.get("date") or it.get("expectedDate") or it.get("openDate")
    close_date = it.get("expectedEndDate") or it.get("expectedDateTo") or it.get("closeDate")
    # price band may be absent from Finnhub; leave None if missing
    price_low = it.get("priceLow") or it.get("price_band_low") or None
    price_high = it.get("priceHigh") or it.get("price_band_high") or None

    # derive a stable key — prefer symbol, else company name
    key_source = (symbol or company or "") .strip().lower().replace(" ", "-")
    key = key_source if key_source else f"finnhub-{int(time.time())}"

    return {
        "key": key,
        "company_name": company,
        "symbol": symbol,
        "exchange": exchange,
        "price_band_low": price_low,
        "price_band_high": price_high,
        "lot_size": None,
        "issue_open_date": open_date,
        "issue_close_date": close_date,
        "status": "upcoming",
        "prospectus_path": None,
        "raw": it
    }

def fetch_finnhub_ipos_window(from_date: str, to_date: str) -> List[dict]:
    """
    Query Finnhub /calendar/ipo for the given YYYY-MM-DD window.
    Returns mapped list of IPO dicts.
    """
    if not FINNHUB_API_KEY:
        print("FINNHUB_API_KEY not set; cannot fetch Finnhub IPO calendar.")
        return []

    params = {"from": from_date, "to": to_date}
    raw = finnhub_get("/calendar/ipo", params)
    if not raw:
        return []

    # save raw payload for provenance
    ts = datetime.utcnow().isoformat()
    finnhub_raw_state.setdefault("queries", []).append({"from": from_date, "to": to_date, "ts": ts, "payload": raw})
    save_json(FINNHUB_RAW_FILE, finnhub_raw_state)

    # The precise JSON shape may vary across Finnhub SDK versions or docs; be defensive
    items = []
    # Keys commonly used: 'data', 'ipoCalendar', or direct list
    if isinstance(raw, dict):
        if "ipoCalendar" in raw and isinstance(raw["ipoCalendar"], list):
            items = raw["ipoCalendar"]
        elif "data" in raw and isinstance(raw["data"], list):
            items = raw["data"]
        else:
            # maybe the payload itself is a list-like dict with numeric keys — try flattening
            items = raw.get("items") or raw.get("results") or []
    elif isinstance(raw, list):
        items = raw
    # If still empty and raw seems list-like, try using raw directly
    if not items and isinstance(raw, list):
        items = raw

    mapped = []
    for it in items:
        if not isinstance(it, dict):
            continue
        mapped.append(map_finnhub_item_to_ipo(it))
    return mapped

# Short helper: fetch next N days window ipos (today..today+days)
def fetch_finnhub_ipos(days_window: int = 7) -> List[dict]:
    today = datetime.utcnow().date()
    to_date = today + timedelta(days=days_window)
    return fetch_finnhub_ipos_window(today.isoformat(), to_date.isoformat())

# ----- GMP aggregator (file-based, same as before) -----
def parse_gmp_from_html_example(html: str) -> Optional[float]:
    import re
    m = re.search(r'GMP[:\s]*₹?\s*([0-9,\-\.]+)', html, flags=re.IGNORECASE)
    if not m:
        return None
    s = m.group(1).replace(",", "")
    try:
        return float(s)
    except:
        return None

PARSERS = {}
def normalize_domain(url: str) -> str:
    from urllib.parse import urlparse
    return urlparse(url).netloc

def gather_gmp_for(ipo_key: str) -> Dict[str, Any]:
    readings = []
    sources_data = []
    for url in GMP_SOURCES:
        try:
            resp = requests.get(url, timeout=6)
            if resp.status_code != 200:
                continue
            domain = normalize_domain(url)
            parser = PARSERS.get(domain, parse_gmp_from_html_example)
            val = parser(resp.text)
            if val is not None:
                ts = datetime.utcnow().isoformat()
                gmp_state.setdefault(ipo_key, []).append({"source": url, "value": val, "ts": ts})
                readings.append(val)
                sources_data.append({"source": url, "value": val, "ts": ts})
        except Exception as e:
            print("GMP source error", url, str(e))
            continue
    save_json(GMP_FILE, gmp_state)
    if not readings:
        return {}
    median = float(statistics.median(readings))
    stdev = float(statistics.pstdev(readings)) if len(readings) > 1 else 0.0
    confidence = max(0.0, 1.0 - (stdev / (abs(median) + 1))) if median != 0 else 0.0
    return {"median": median, "sources": sources_data, "confidence": confidence}

# ----- Prospectus helper (optional) -----
def extract_fundamentals_from_pdf(local_pdf_path: str) -> Dict[str, Any]:
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber not installed; skipping prospectus parse.")
        return {}
    if not local_pdf_path or not os.path.exists(local_pdf_path):
        return {}
    tables_combined = []
    with pdfplumber.open(local_pdf_path) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables()
                for t in tables:
                    if t and len(t) > 0:
                        header = t[0]
                        rows = t[1:]
                        for r in rows:
                            if len(header) == len(r):
                                rowdict = dict(zip(header, r))
                                tables_combined.append(rowdict)
            except Exception:
                continue
    out = {}
    for row in tables_combined:
        for k, v in row.items():
            key_lower = str(k).lower()
            if "revenue" in key_lower or "net sales" in key_lower:
                out.setdefault("revenue_rows", []).append(row)
            if "ebitda" in key_lower:
                out.setdefault("ebitda_rows", []).append(row)
            if "profit" in key_lower:
                out.setdefault("profit_rows", []).append(row)
    return out

# ----- Notifier (Telegram) -----
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram token/chat not set; skipping send. Message would be:\n", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=8)
        if r.status_code != 200:
            print("Telegram send failed:", r.status_code, r.text)
    except Exception as e:
        print("Telegram send exception:", e)

# ----- Core logic: detect changes and notify -----
def detect_and_notify(current_ipos: List[Dict[str, Any]]):
    changed = False
    for ipo in current_ipos:
        key = ipo["key"]
        prev = ipos_state.get(key)
        if not prev:
            # new IPO
            ipos_state[key] = ipo.copy()
            ipos_state[key]["seen_at"] = datetime.utcnow().isoformat()
            save_json(IPOS_FILE, ipos_state)
            changed = True
            msg = ("📢 *New IPO detected*\n"
                   f"{ipo.get('company_name')} ({ipo.get('symbol') or ''})\n"
                   f"Open: {ipo.get('issue_open_date')} — Close: {ipo.get('issue_close_date')}\n"
                   f"Price band: ₹{ipo.get('price_band_low') or 'N/A'} - ₹{ipo.get('price_band_high') or 'N/A'}\n"
                   f"Exchange: {ipo.get('exchange') or 'N/A'}\n")
            send_telegram(msg + "\n_Disclaimer: not investment advice._")
        else:
            # existing IPO: check status change
            if prev.get("status") != ipo.get("status"):
                old = prev.get("status")
                ipos_state[key].update(ipo)
                ipos_state[key]["updated_at"] = datetime.utcnow().isoformat()
                save_json(IPOS_FILE, ipos_state)
                changed = True
                msg = ("🔄 *IPO status changed*\n"
                       f"{ipo.get('company_name')} ({ipo.get('symbol') or ''})\n"
                       f"{old} → {ipo.get('status')}\n")
                send_telegram(msg + "\n_Disclaimer: not investment advice._")

        # gather GMP and check threshold
        agg = gather_gmp_for(key)
        if agg:
            median = agg.get("median")
            conf = agg.get("confidence", 0.0)
            pm = prev.get("last_notified_gmp") if prev else None
            if median is not None:
                should_notify = False
                if median >= GMP_NOTIFY_THRESHOLD:
                    if not pm or float(pm) < GMP_NOTIFY_THRESHOLD:
                        should_notify = True
                if pm and abs(float(pm) - median) / (abs(float(pm)) + 1e-9) > 0.5:
                    should_notify = True
                if should_notify:
                    ipos_state.setdefault(key, {}).update({"last_notified_gmp": median})
                    save_json(IPOS_FILE, ipos_state)
                    midpoint = "N/A"
                    try:
                        low = float(ipos_state[key].get('price_band_low') or 0)
                        high = float(ipos_state[key].get('price_band_high') or 0)
                        midpoint = (low + high) / 2 if (low or high) else 0
                    except:
                        midpoint = "N/A"
                    msg = (f"📈 *GMP Alert* — {ipos_state[key].get('company_name')}\n"
                           f"GMP (median): ₹{median:.1f} (confidence {conf:.2f})\n"
                           f"Price band midpoint: ₹{midpoint}\n")
                    send_telegram(msg + "\n_Disclaimer: GMP is informal; not investment advice._")

    save_json(GMP_FILE, gmp_state)
    save_json(FUND_FILE, funds_state)
    return changed

# ----- Main loop -----
def main_loop():
    print("Zero-DB IPO Agent with Finnhub starting. Data dir:", DATA_DIR)
    save_json(IPOS_FILE, ipos_state)
    save_json(GMP_FILE, gmp_state)
    save_json(FUND_FILE, funds_state)
    save_json(FINNHUB_RAW_FILE, finnhub_raw_state)

    while True:
        try:
            # fetch next 7 days by default
            current = fetch_finnhub_ipos(days_window=7)
            if not current:
                print("No IPOs from Finnhub this cycle.")
            else:
                detect_and_notify(current)
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main_loop()
