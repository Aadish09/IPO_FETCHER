#!/usr/bin/env python3
"""
Zero-DB IPO Agent (IPOAlerts 1-page test fetch + Investorgain GMP scraper)

Notes:
- Temporarily limited IPOAlerts calls to a single page (page=1) for testing.
- Once validated, we can restore multi-page behaviour or make it configurable.
- Investorgain scraper and in-memory GMP aggregation remain unchanged.
- Usage same as before.

Usage:
  pip install requests beautifulsoup4
  # optionally for Playwright rendering:
  pip install playwright bs4
  python -m playwright install --with-deps

  export IPOALERTS_API_KEY="..."
  export TELEGRAM_BOT_TOKEN="..."
  export TELEGRAM_CHAT_ID="..."
  python zero_db_ipo_agent.py --once
"""

from __future__ import annotations
import os
import json
import time
import argparse
import statistics
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

# -------------------------
# Config (env / defaults)
# -------------------------
POLL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # fallback poll interval
DATA_DIR = os.path.join(os.getcwd(), "data")
IPOS_FILE = os.path.join(DATA_DIR, "ipos.json")
FUND_FILE = os.path.join(DATA_DIR, "fundamentals.json")
IPOALERTS_RAW_FILE = os.path.join(DATA_DIR, "ipoalerts_raw.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IPOALERTS_API_KEY = os.getenv("IPOALERTS_API_KEY")

# Investorgain GMP page (live GMP listing)
INVESTORGAIN_GMP_URL = "https://www.investorgain.com/report/live-ipo-gmp/331/"

# Replace/add extra GMP source URLs here (optional); these will be parsed with the simple parser
GMP_SOURCES = [
    # "https://example-gmp-source-1.com/ipo/gmp/example-corp",
]

# notify if GMP median >= threshold (example)
GMP_NOTIFY_THRESHOLD = float(os.getenv("GMP_NOTIFY_THRESHOLD", "50.0"))

# IPOAlerts endpoint
IPOALERTS_BASE = "https://api.ipoalerts.in"
IPOALERTS_IPOS_PATH = "/ipos"

# polite pause between API calls (seconds)
API_CALL_SLEEP = float(os.getenv("API_CALL_SLEEP", "1.0"))

# -------------------------
# File utilities
# -------------------------
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_json(path: str, obj: Dict[str, Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)

# -------------------------
# In-memory state (file-backed for IPOs only)
# -------------------------
ensure_data_dir()
ipos_state: Dict[str, Any] = load_json(IPOS_FILE)
funds_state: Dict[str, Any] = load_json(FUND_FILE) or {}
ipoalerts_raw_state: Dict[str, Any] = load_json(IPOALERTS_RAW_FILE) or {}

# -------------------------
# IPOAlerts: fetch (TEST MODE: single page only)
# -------------------------
def fetch_ipoalerts_15_pages() -> List[Dict[str, Any]]:
    """
    TEST MODE: Calls IPOAlerts API once with:
      GET /ipos?status=open&limit=1&page=1

    Reads API key from IPOALERTS_API_KEY env var (x-api-key header).
    Returns a list of normalized IPO dicts (internal shape expected by detect_and_notify).
    """
    if not IPOALERTS_API_KEY:
        print("IPOALERTS_API_KEY not set — cannot call IPOAlerts API.")
        return []

    headers = {
        "x-api-key": IPOALERTS_API_KEY,
        "Content-Type": "application/json"
    }

    collected: List[Dict[str, Any]] = []
    ts_run = datetime.utcnow().isoformat()
    ipoalerts_raw_state.setdefault("runs", []).append({"ts": ts_run, "pages_called": []})

    # TEST MODE: only call page 1 once. Change range(1,2) -> range(1,16) to restore 15-page behaviour.
    for page in range(1, 2):  # pages 1..1 (single call for testing)
        params = {"status": "open", "limit": 1, "page": page}
        url = f"{IPOALERTS_BASE}{IPOALERTS_IPOS_PATH}"
        try:
            print(f"Calling IPOAlerts page={page} ...")
            resp = requests.get(url, headers=headers, params=params, timeout=12)
            ipoalerts_raw_state["runs"][-1]["pages_called"].append({"page": page, "status_code": resp.status_code})
            if resp.status_code != 200:
                print(f"Warning: IPOAlerts page {page} returned {resp.status_code}: {resp.text[:200]}")
                time.sleep(API_CALL_SLEEP)
                continue
            payload = resp.json()
            items = payload.get("data") or []
            if not isinstance(items, list):
                print(f"Warning: unexpected payload on page {page}, skipping.")
                time.sleep(API_CALL_SLEEP)
                continue

            for it in items:
                company = it.get("company_name") or it.get("name") or it.get("company")
                symbol = it.get("symbol")
                price_low = it.get("price_low") or it.get("min_price") or None
                price_high = it.get("price_high") or it.get("max_price") or None
                open_date = it.get("open_date") or it.get("issue_open_date") or None
                close_date = it.get("close_date") or it.get("issue_close_date") or None
                lot_size = it.get("lot_size") or None
                exchange = it.get("exchange") or it.get("market") or "NSE/BSE"
                status = it.get("status") or "open"

                key_source = (symbol or company or "").strip().lower().replace(" ", "-")
                key = key_source or f"ipoalerts-{int(time.time())}-{page}"

                normalized = {
                    "key": key,
                    "company_name": company,
                    "symbol": symbol,
                    "exchange": exchange,
                    "price_band_low": price_low,
                    "price_band_high": price_high,
                    "lot_size": lot_size,
                    "issue_open_date": open_date,
                    "issue_close_date": close_date,
                    "status": status,
                    "prospectus_path": it.get("prospectus_url") or None,
                    "raw": it
                }
                collected.append(normalized)

        except requests.RequestException as e:
            print(f"Error fetching IPOAlerts page {page}: {e}")
        except Exception as e:
            print(f"Unexpected error on page {page}: {e}")
        time.sleep(API_CALL_SLEEP)

    ipoalerts_raw_state["runs"][-1]["collected"] = len(collected)
    try:
        save_json(IPOALERTS_RAW_FILE, ipoalerts_raw_state)
    except Exception:
        pass

    return collected

# -------------------------
# Investorgain scraper (requests + BS fallback; optional Playwright)
# -------------------------
INVESTORGAIN_HEADERS = {
    "User-Agent": "ipo-agent/1.0 (contact: you@example.com)"
}
import re
_num_re = re.compile(r'[-+]?\d{1,3}(?:[,\d{3}]*)?(?:\.\d+)?')

def parse_currency_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = str(s)
    s = s.replace("₹", "").replace("Rs.", "").replace("INR", "")
    s = s.replace("+", "").replace("−", "-").replace("—", "-")
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    m = _num_re.search(s)
    if not m:
        return None
    num = m.group(0).replace(",", "")
    try:
        return float(num)
    except Exception:
        return None

from bs4 import BeautifulSoup

def fetch_investorgain_gmp_requests() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        r = requests.get(INVESTORGAIN_GMP_URL, headers=INVESTORGAIN_HEADERS, timeout=12)
        if r.status_code != 200:
            print("investorgain: HTTP", r.status_code)
            return out
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        table = None
        headings = soup.find_all(lambda tag: tag.name in ("h1", "h2", "h3", "h4") and "gmp" in tag.get_text(strip=True).lower())
        for h in headings:
            t = h.find_next("table")
            if t:
                table = t
                break

        if table is None:
            candidate_tables = soup.find_all("table")
            for t in candidate_tables:
                headers_text = " ".join([th.get_text(" ", strip=True).lower() for th in t.find_all("th")])
                if "gmp" in headers_text or "premium" in headers_text or "grey market" in headers_text:
                    table = t
                    break
            if table is None and candidate_tables:
                table = candidate_tables[0]

        if table is None:
            return out

        rows = table.find_all("tr")
        for tr in rows:
            cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cols or len(cols) < 2:
                continue
            if tr.find("th"):
                hdr = " ".join(cols).lower()
                if "company" in hdr and ("gmp" in hdr or "premium" in hdr):
                    continue
            company = None
            gmp_val = None
            gmp_raw = None
            for c in cols:
                if company is None and re.search(r"[A-Za-z]", c) and not _num_re.fullmatch(c.replace(",", "")):
                    company = c
            for c in reversed(cols):
                parsed = parse_currency_to_float(c)
                if parsed is not None:
                    gmp_val = parsed
                    gmp_raw = c
                    break
            if not company and cols:
                company = cols[0]
            key = (company or "").strip().lower().replace(" ", "-")
            out[key] = {
                "company_name": (company or "").strip(),
                "gmp_value": gmp_val,
                "gmp_raw": gmp_raw,
                "source": INVESTORGAIN_GMP_URL,
                "ts": datetime.utcnow().isoformat()
            }
        return out
    except Exception as e:
        print("investorgain requests parse error:", e)
        return {}

def fetch_investorgain_gmp_playwright() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return out

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=INVESTORGAIN_HEADERS["User-Agent"])
            page.goto(INVESTORGAIN_GMP_URL, timeout=30000)
            try:
                page.wait_for_selector("table, .report_content, #main", timeout=10000)
            except Exception:
                pass
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, "html.parser")
        table = None
        headings = soup.find_all(lambda tag: tag.name in ("h1", "h2", "h3", "h4") and "gmp" in tag.get_text(strip=True).lower())
        for h in headings:
            t = h.find_next("table")
            if t:
                table = t
                break
        if table is None:
            candidate_tables = soup.find_all("table")
            if candidate_tables:
                table = candidate_tables[0]
        if not table:
            return out

        rows = table.find_all("tr")
        for tr in rows:
            cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cols or len(cols) < 2:
                continue
            if tr.find("th"):
                hdr = " ".join(cols).lower()
                if "company" in hdr and ("gmp" in hdr or "premium" in hdr):
                    continue
            company = None
            gmp_val = None
            gmp_raw = None
            for c in cols:
                if company is None and re.search(r"[A-Za-z]", c) and not _num_re.fullmatch(c.replace(",", "")):
                    company = c
            for c in reversed(cols):
                parsed = parse_currency_to_float(c)
                if parsed is not None:
                    gmp_val = parsed
                    gmp_raw = c
                    break
            if not company and cols:
                company = cols[0]
            key = (company or "").strip().lower().replace(" ", "-")
            out[key] = {
                "company_name": (company or "").strip(),
                "gmp_value": gmp_val,
                "gmp_raw": gmp_raw,
                "source": INVESTORGAIN_GMP_URL,
                "ts": datetime.utcnow().isoformat()
            }
        return out
    except Exception as e:
        print("investorgain playwright parse error:", e)
        return {}

def fetch_investorgain_gmp() -> Dict[str, Dict[str, Any]]:
    snap = fetch_investorgain_gmp_requests()
    if snap:
        return snap
    try:
        snap2 = fetch_investorgain_gmp_playwright()
        if snap2:
            return snap2
    except Exception:
        pass
    return {}

# -------------------------
# GMP aggregator (in-memory only, no persistence)
# -------------------------
def gather_gmp_for(ipo_key: str) -> Dict[str, Any]:
    readings: List[float] = []
    sources_data: List[Dict[str, Any]] = []

    try:
        snap = fetch_investorgain_gmp()
        rec = snap.get(ipo_key)
        if rec and rec.get("gmp_value") is not None:
            val = rec["gmp_value"]
            readings.append(val)
            sources_data.append({"source": rec["source"], "value": val, "ts": rec["ts"], "provider": "investorgain"})
    except Exception as e:
        print("Error fetching Investorgain snapshot:", e)

    for url in GMP_SOURCES:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue
            parser = PARSERS.get(normalize_domain(url), None)
            if parser is None:
                val = parse_gmp_from_html_example(r.text)
            else:
                val = parser(r.text)
            if val is not None:
                readings.append(val)
                sources_data.append({"source": url, "value": val, "ts": datetime.utcnow().isoformat(), "provider": "custom"})
        except Exception as e:
            print("Error scraping GMP source", url, e)
            continue

    if not readings:
        return {}
    median = float(statistics.median(readings))
    stdev = float(statistics.pstdev(readings)) if len(readings) > 1 else 0.0
    confidence = max(0.0, 1.0 - (stdev / (abs(median) + 1))) if median != 0 else 0.0
    return {"median": median, "sources": sources_data, "confidence": confidence}

# -------------------------
# Telegram notifier
# -------------------------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured — message would be:\n", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=8)
        if r.status_code != 200:
            print("Telegram send failed:", r.status_code, r.text)
    except Exception as e:
        print("Telegram send exception:", e)

# -------------------------
# Core: detect and notify (collect notifications, don't send inline)
# -------------------------
def map_ipo_to_key(ipo_item: Dict[str, Any]) -> str:
    symbol = ipo_item.get("symbol")
    company = ipo_item.get("company_name") or ipo_item.get("name") or ipo_item.get("company")
    slug = ipo_item.get("slug") or ipo_item.get("id")
    base = (symbol or company or slug or "").strip().lower().replace(" ", "-")
    return base or f"ipo-{int(time.time())}"

def detect_and_notify(current_ipos: List[Dict[str, Any]]) -> List[str]:
    notifications: List[str] = []
    ts = datetime.utcnow().isoformat()

    for ipo in current_ipos:
        key = ipo["key"]
        prev = ipos_state.get(key)
        if not prev:
            ipos_state[key] = ipo.copy()
            ipos_state[key]["seen_at"] = ts
            save_json(IPOS_FILE, ipos_state)
            notifications.append(
                f"📢 *New IPO:* {ipo.get('company_name')} ({ipo.get('symbol') or '—'})\n"
                f"Open: {ipo.get('issue_open_date') or 'N/A'} — Close: {ipo.get('issue_close_date') or 'N/A'}\n"
                f"Price band: ₹{ipo.get('price_band_low') or 'N/A'}–₹{ipo.get('price_band_high') or 'N/A'}\n"
            )
        else:
            if str(prev.get("status")) != str(ipo.get("status")):
                old = prev.get("status")
                ipos_state[key].update(ipo)
                ipos_state[key]["updated_at"] = ts
                save_json(IPOS_FILE, ipos_state)
                notifications.append(
                    f"🔄 *Status change:* {ipo.get('company_name')} ({ipo.get('symbol') or '—'})\n"
                    f"{old} → {ipo.get('status')}\n"
                )

        # GMP aggregation using in-memory aggregator (no persistence)
        agg = gather_gmp_for(key)
        if agg:
            median = agg.get("median")
            conf = agg.get("confidence", 0.0)
            pm = prev.get("last_notified_gmp") if prev else None
            if median is not None:
                should_notify = False
                if median >= GMP_NOTIFY_THRESHOLD and (not pm or float(pm) < GMP_NOTIFY_THRESHOLD):
                    should_notify = True
                if pm and abs(float(pm) - median) / (abs(float(pm)) + 1e-9) > 0.5:
                    should_notify = True
                if should_notify:
                    # note: we do not persist detailed GMP history; only track last notified GMP in ipos_state
                    ipos_state.setdefault(key, {}).update({"last_notified_gmp": median})
                    save_json(IPOS_FILE, ipos_state)
                    midpoint = "N/A"
                    try:
                        low = float(ipos_state[key].get('price_band_low') or 0)
                        high = float(ipos_state[key].get('price_band_high') or 0)
                        midpoint = (low + high) / 2 if (low or high) else 0
                    except:
                        midpoint = "N/A"
                    notifications.append(
                        f"📈 *GMP Alert:* {ipos_state[key].get('company_name')}\n"
                        f"GMP (median): ₹{median:.1f} (confidence {conf:.2f})\n"
                        f"Price band midpoint: ₹{midpoint}\n"
                    )

    # no saving of GMP state to disk here (GMP aggregations are ephemeral)
    save_json(FUND_FILE, funds_state)
    return notifications

# -------------------------
# Main loop + CLI arg parsing
# -------------------------
def run_cycle():
    current = fetch_ipoalerts_15_pages()
    if not current:
        print("No IPOs returned by IPOAlerts this cycle.")
        return

    notifications = detect_and_notify(current)

    if notifications:
        header = f"*IPO Agent update — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}*\n\n"
        body = "\n---\n".join(notifications)
        footer = "\n\n_Disclaimer: not investment advice._"
        message = header + body + footer
        max_len = 3800
        if len(message) <= max_len:
            send_telegram(message)
        else:
            parts: List[str] = []
            cur = header
            for note in notifications:
                chunk = ("\n---\n" + note) if cur != header else note
                if len(cur) + len(chunk) + len(footer) > max_len:
                    parts.append(cur + footer)
                    cur = header + note
                else:
                    cur += ("\n---\n" + note) if cur != header else note
            parts.append(cur + footer)
            for p in parts:
                send_telegram(p)
    else:
        print("No notifications this cycle.")

def main_loop(one_shot: bool = False):
    print("Zero-DB IPO Agent starting. Data dir:", DATA_DIR)
    ensure_data_dir()
    save_json(IPOS_FILE, ipos_state)
    save_json(FUND_FILE, funds_state)
    save_json(IPOALERTS_RAW_FILE, ipoalerts_raw_state)

    if one_shot:
        run_cycle()
        print("One-shot run complete — exiting.")
        return

    while True:
        try:
            run_cycle()
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zero-DB IPO Agent")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit (good for CI)")
    args = parser.parse_args()
    ONE_SHOT = args.once or os.getenv("ONE_SHOT") == "1"
    main_loop(one_shot=ONE_SHOT)
