"""
fetch.py — primary data fetcher (v2: direct 5-minute bucket recording).

Runs every 5 minutes (see .github/workflows/fetch.yml). For each pair listed
in pairs.json:
  1. Calls DexScreener's public REST API and reads txns.m5 — this is
     DexScreener's own count of buys/sells in the last 5 minutes, computed
     directly from their indexed transaction data. No estimation needed.
  2. Records that as ONE clean 5-minute data point, timestamped to the
     5-minute window it represents.
  3. Appends it to fetcher/storage/raw/<chain>_<pair>.json and prunes
     anything older than 30 days.

Why this replaces the old "diff the h1 rolling window" approach: DexScreener's
h1/h6/h24 fields are ROLLING windows that can decrease between polls (as old
trades age out of the window) even while new trades are happening. Diffing
them produced incorrect, sometimes-negative-then-clamped-to-zero deltas.
txns.m5 has no such problem — as long as we poll at least once every 5
minutes, each poll's m5 value cleanly represents that specific 5-minute
slice with no overlap and no guessing.

aggregate.py then combines these 5-minute buckets upward into 15m/1h/4h/1d.
"""

import json
import os
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
BUCKET_SECONDS = 5 * 60  # this fetcher's native resolution: 5 minutes


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
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_id}/{pair_address}"
    data = http_get_json(url)
    pairs = data.get("pairs") or []
    if not pairs:
        return None
    return pairs[0]


def fetch_pair_sniffed(chain_id, pair_address, sniff_cfg):
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


def bucket_start(ts, size_seconds=BUCKET_SECONDS):
    return int(ts // size_seconds) * size_seconds


def prune_old(records, days=RETENTION_DAYS):
    cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
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
    m5 = txns.get("m5", {}) or {}
    buys = m5.get("buys", 0)
    sells = m5.get("sells", 0)

    now = datetime.now(timezone.utc)
    bucket_ts = bucket_start(now.timestamp())
    path = raw_path(chain_id, pair_address)
    records = load_json(path, [])

    records = [r for r in records if bucket_start(r["ts"]) != bucket_ts]

    record = {
        "ts": bucket_ts,
        "iso": datetime.fromtimestamp(bucket_ts, tz=timezone.utc).isoformat(),
        "new_buys": buys,
        "new_sells": sells,
        "new_txns": buys + sells,
        "priceUsd": pair_data.get("priceUsd"),
        "source": "sniffed" if used_fallback else "public_api",
    }
    records.append(record)
    records.sort(key=lambda r: r["ts"])
    records = prune_old(records)
    save_json(path, records)
    log(f"  5m bucket: {buys} buys, {sells} sells (source={record['source']})")

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
        time.sleep(1)

    save_json(failures_path, failures)

    needs_sniff = any(v >= MAX_FAILURES_BEFORE_SNIFF for v in failures.values())
    marker = os.path.join(BASE_DIR, "storage", "NEEDS_SNIFF")
    if needs_sniff:
        with open(marker, "w") as f:
            f.write("1")
        log("Some pairs are failing repeatedly — flagged for sniff fallback.")
    elif os.path.exists(marker):
        os.remove(marker)


if __name__ == "__main__":
    main()
