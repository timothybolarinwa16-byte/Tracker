"""
aggregate.py — turns raw 15-min delta snapshots (fetcher/storage/raw/*.json)
into bucketed timeframe series that chart.html can render directly.

Output: fetcher/storage/aggregated/<chain>_<pair>.json
{
  "meta": {...},
  "generated_at": "...",
  "timeframes": {
    "15m": [{ "t": <bucket_start_iso>, "txns": N, "buys": N, "sells": N }, ...],
    "1h":  [...],
    "4h":  [...],
    "1d":  [...]
  }
}

Bucket boundaries are aligned to UTC epoch (e.g. hourly buckets start on the
hour), and a raw record's counts are attributed to the bucket containing its
timestamp.
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "storage", "raw")
AGG_DIR = os.path.join(BASE_DIR, "storage", "aggregated")
META_FILE = os.path.join(BASE_DIR, "storage", "meta.json")

TIMEFRAMES_SECONDS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}


def log(msg):
    print(f"[aggregate {datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


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


def bucket_start(ts, size_seconds):
    return int(ts // size_seconds) * size_seconds


def bucketize(records, size_seconds):
    buckets = {}
    for r in records:
        b = bucket_start(r["ts"], size_seconds)
        if b not in buckets:
            buckets[b] = {"buys": 0, "sells": 0, "txns": 0}
        buckets[b]["buys"] += r.get("new_buys", 0)
        buckets[b]["sells"] += r.get("new_sells", 0)
        buckets[b]["txns"] += r.get("new_txns", 0)

    out = []
    for b in sorted(buckets.keys()):
        out.append({
            "t": datetime.fromtimestamp(b, tz=timezone.utc).isoformat(),
            **buckets[b],
        })
    return out


def main():
    if not os.path.isdir(RAW_DIR):
        log("No raw data yet.")
        return

    meta_all = load_json(META_FILE, {})

    for fname in os.listdir(RAW_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(RAW_DIR, fname)
        records = load_json(path, [])
        if not records:
            continue

        key_stub = fname[:-5]  # chain_pairaddress
        # find matching meta entry (key format is "chain:pair")
        meta = None
        for k, v in meta_all.items():
            if k.replace(":", "_") == key_stub:
                meta = v
                break

        timeframes = {
            tf: bucketize(records, secs) for tf, secs in TIMEFRAMES_SECONDS.items()
        }

        output = {
            "meta": meta or {},
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "timeframes": timeframes,
        }
        out_path = os.path.join(AGG_DIR, fname)
        save_json(out_path, output)
        log(f"Aggregated {fname}: {sum(len(v) for v in timeframes.values())} total buckets")


if __name__ == "__main__":
    main()
