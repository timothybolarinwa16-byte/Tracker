"""
sniff.py — fallback endpoint discovery.

Only runs when fetch.py has flagged repeated public-API failures
(fetcher/storage/NEEDS_SNIFF exists). Requires Playwright + a Chromium
browser, which the GitHub Actions workflow installs on-demand — this keeps
normal runs fast/cheap since Playwright is NOT installed every run.

What it does:
  1. Launches headless Chromium, opens a DexScreener pair page for one of
     the failing pairs, and records every XHR/fetch request the page makes.
  2. Filters for requests that look like they return per-pair txn/stat data
     (JSON responses containing "buys"/"sells" or "txns").
  3. Saves the first matching URL as a template (with the chain/pair
     substituted back to {chainId}/{pairAddress} placeholders) plus any
     required headers into sniff_config.json, so fetch.py can call it
     directly next time without needing a browser.
  4. Verifies the discovered pattern still returns valid JSON before saving.

If Playwright/Chromium aren't available, or nothing is found, it exits
quietly and fetch.py continues in public-API-only mode.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PAIRS_FILE = os.path.join(BASE_DIR, "pairs.json")
SNIFF_CONFIG_FILE = os.path.join(BASE_DIR, "sniff_config.json")
FAILURES_FILE = os.path.join(BASE_DIR, "storage", "failures.json")


def log(msg):
    print(f"[sniff {datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


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


def pick_target_pair():
    """Choose the pair with the most consecutive failures to sniff against."""
    pairs = load_json(PAIRS_FILE, {"pairs": []}).get("pairs", [])
    failures = load_json(FAILURES_FILE, {})
    if not pairs:
        return None
    def fail_count(p):
        key = f"{p['chainId']}:{p['pairAddress']}"
        return failures.get(key, 0)
    pairs.sort(key=fail_count, reverse=True)
    return pairs[0]


def looks_like_txn_data(body_text):
    if not body_text:
        return False
    try:
        obj = json.loads(body_text)
    except Exception:
        return False
    blob = json.dumps(obj).lower()
    return ("buys" in blob and "sells" in blob) or "txns" in blob


def sniff(chain_id, pair_address):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("Playwright not installed — skipping sniff (public API only).")
        return None

    candidate_urls = []
    page_url = f"https://dexscreener.com/{chain_id}/{pair_address}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))

        def on_response(response):
            try:
                url = response.url
                if "dexscreener" not in url and "dex-screener" not in url:
                    return
                ctype = response.headers.get("content-type", "")
                if "json" not in ctype:
                    return
                body = response.text()
                if looks_like_txn_data(body):
                    candidate_urls.append(url)
            except Exception:
                pass

        page.on("response", on_response)
        try:
            page.goto(page_url, timeout=30000, wait_until="networkidle")
            page.wait_for_timeout(3000)
        except Exception as e:
            log(f"page load failed: {e}")
        browser.close()

    if not candidate_urls:
        log("No matching JSON endpoints observed.")
        return None

    chosen = candidate_urls[0]
    # Turn the concrete URL into a reusable template by substituting the
    # known chain/pair values back to placeholders.
    template = chosen.replace(pair_address, "{pairAddress}").replace(chain_id, "{chainId}")
    log(f"Discovered candidate endpoint: {template}")
    return template


def main():
    target = pick_target_pair()
    if not target:
        log("No tracked pairs — nothing to sniff.")
        return

    cfg = load_json(SNIFF_CONFIG_FILE, {})
    template = sniff(target["chainId"], target["pairAddress"])

    if template:
        cfg["url_template"] = template
        cfg["discovered_at"] = datetime.now(timezone.utc).isoformat()
        cfg["last_verified_at"] = cfg["discovered_at"]
        cfg["consecutive_failures"] = 0
        save_json(SNIFF_CONFIG_FILE, cfg)
        log("Saved new endpoint template to sniff_config.json")
    else:
        cfg["consecutive_failures"] = cfg.get("consecutive_failures", 0) + 1
        save_json(SNIFF_CONFIG_FILE, cfg)
        log("Sniff failed to find a usable endpoint this run.")


if __name__ == "__main__":
    main()
