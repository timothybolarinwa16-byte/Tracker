"""
fetch.py — primary data fetcher.

Runs on a schedule (see .github/workflows/fetch.yml). For each pair listed in
pairs.json:
  1. Calls DexScreener's public REST API to get current txns.h1 buys/sells
     (a rolling-window cumulative count, NOT raw transaction events —
     DexScreener's public API does not expose transaction history).
  2. Diffs against the previous snapshot to estimate the number of NEW buys/
     sells since the last poll. This is an approximation: it assumes the
     h1 rolling window (60 min) is much larger than the poll interval
     (15 min), so buys(t) - buys(t-15min) ~= new buys in the last 15 min.
     It can drift slightly if the window boundary and poll times misalign,
     but it's the only viable method without raw event data.
  3. Appends a timestamped delta record to fetcher/storage/raw/<chain>_<pair>.json
  4. Prunes anything older than 30 days.
  5. If the public API call fails repeatedly for a pair, flags it so
     sniff.py can be invoked as a fallback (see fetch.yml).

No third-party deps beyond `requests` (stdlib otherwise).
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PAIRS_FILE = os.path.join(BASE_DIR, "pairs.json")
SNIFF_CONFIG_FILE = os.path.join(BASE_DIR, "sniff_config.json")
RAW_DIR = os.path.join(BASE_DIR, "storage", "raw")
RETENTION_DAYS = 30
MAX_FAILURES_BEFORE_SNIFF = 3
USER_AGENT = "Mozilla/5.0 (compatible; dex-tx-tracker/1.0)"


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def http_get_json(url, headers=None):
    req = Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_pair_public_api(chain_id, pair_address):
    """Primary path: DexScreener public REST API."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_id}/{pair_address}"
    data = http_get_json(url)
    pairs = data.get("pairs") or []
    if not pairs:
        return None
    return pairs[0]


def fetch_pair_sniffed(chain_id, pair_address, sniff_cfg):
    """Fallback path: use the endpoint pattern sniff.py discovered, if any."""
    template = sniff_cfg.get("url_template")
    if not template:
        return None
    url = template.format(chainId=chain_id, pairAddress=pair_address)
    try:
        return http_get_json(url, headers=sniff_cfg.get("headers") or {})
    except Exception as e:
        log(f"  sniffed endpoint failed: {e}")
        return None


def raw_path(chain_id, pair_address):
    return os.path.join(RAW_DIR, f"{chain_id}_{pair_address}.json")


def prune_old(records, days=RETENTION_DAYS):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_ts = cutoff.timestamp()
    return [r for r in records if r["ts"] >= cutoff_ts]


def process_pair(entry, sniff_cfg, failures):
    chain_id = entry["chainId"]
    pair_address = entry["pairAddress"]
    key = f"{chain_id}:{pair_address}"
    log(f"Fetching {key}")

    pair_data = None
    try:
        pair_data = fetch_pair_public_api(chain_id, pair_address)
    except (URLError, HTTPError) as e:
        log(f"  public API failed: {e}")

    used_fallback = False
    if pair_data is None:
        pair_data = fetch_pair_sniffed(chain_id, pair_address, sniff_cfg)
        used_fallback = pair_data is not None

    if pair_data is None:
        failures[key] = failures.get(key, 0) + 1
        log(f"  no data (consecutive failures: {failures[key]})")
        return

    failures[key] = 0
    txns = pair_data.get("txns", {})
    h1 = txns.get("h1", {}) or {}
    m5 = txns.get("m5", {}) or {}

    now = datetime.now(timezone.utc)
    path = raw_path(chain_id, pair_address)
    records = load_json(path, [])

    prev = records[-1] if records else None
    cum_buys_h1 = h1.get("buys", 0)
    cum_sells_h1 = h1.get("sells", 0)

    if prev:
        delta_buys = max(0, cum_buys_h1 - prev.get("cum_buys_h1", 0))
        delta_sells = max(0, cum_sells_h1 - prev.get("cum_sells_h1", 0))
        # If cumulative counters reset (e.g. a gap in polling longer than the
        # 1h window), fall back to the m5 snapshot as a rough same-interval proxy.
        gap_minutes = (now.timestamp() - prev["ts"]) / 60
        if gap_minutes > 55:
            delta_buys = m5.get("buys", 0)
            delta_sells = m5.get("sells", 0)
    else:
        # First ever snapshot for this pair: seed with m5 as a reasonable estimate.
        delta_buys = m5.get("buys", 0)
        delta_sells = m5.get("sells", 0)

    record = {
        "ts": now.timestamp(),
        "iso": now.isoformat(),
        "new_buys": delta_buys,
        "new_sells": delta_sells,
        "new_txns": delta_buys + delta_sells,
        "cum_buys_h1": cum_buys_h1,
        "cum_sells_h1": cum_sells_h1,
        "priceUsd": pair_data.get("priceUsd"),
        "source": "sniffed" if used_fallback else "public_api",
    }
    records.append(record)
    records = prune_old(records)
    save_json(path, records)
    log(f"  +{delta_buys} buys, +{delta_sells} sells (source={record['source']})")

    # keep lightweight token/pair metadata alongside for chart.html labels
    meta_path = os.path.join(BASE_DIR, "storage", "meta.json")
    meta = load_json(meta_path, {})
    meta[key] = {
        "chainId": chain_id,
        "pairAddress": pair_address,
        "baseSymbol": (pair_data.get("baseToken") or {}).get("symbol"),
        "quoteSymbol": (pair_data.get("quoteToken") or {}).get("symbol"),
        "dexId": pair_data.get("dexId"),
        "url": pair_data.get("url"),
        "updated_at": now.isoformat(),
    }
    save_json(meta_path, meta)


def main():
    pairs_cfg = load_json(PAIRS_FILE, {"pairs": []})
    sniff_cfg = load_json(SNIFF_CONFIG_FILE, {})
    failures_path = os.path.join(BASE_DIR, "storage", "failures.json")
    failures = load_json(failures_path, {})

    pairs = pairs_cfg.get("pairs", [])
    if not pairs:
        log("No pairs tracked yet (fetcher/pairs.json is empty). Nothing to do.")
        return

    for entry in pairs:
        try:
            process_pair(entry, sniff_cfg, failures)
        except Exception as e:
            log(f"Unexpected error for {entry}: {e}")
        time.sleep(1)  # be polite to the API

    save_json(failures_path, failures)

    needs_sniff = any(v >= MAX_FAILURES_BEFORE_SNIFF for v in failures.values())
    # Signal to the GitHub Actions workflow (via a marker file) that sniff.py
    # should run this time.
    marker = os.path.join(BASE_DIR, "storage", "NEEDS_SNIFF")
    if needs_sniff:
        with open(marker, "w") as f:
            f.write("1")
        log("Some pairs are failing repeatedly — flagged for sniff fallback.")
    elif os.path.exists(marker):
        os.remove(marker)


if __name__ == "__main__":
    main()
