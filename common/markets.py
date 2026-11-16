"""Maps Token ID → market info (question, outcome, resolution), cached."""

import time
import requests

TOKENS_URL = "https://data-api.polymarket.com/v2/tokens"
BATCH = 50
TTL_UNRESOLVED = 6 * 3600  # Refresh resolved status after 6h
_CACHE: dict[str, dict] = {}  # Cache already seen tokens


def _is_stale(entry: dict) -> bool:
    """Returns if a token entry needs its resolution status refreshed"""
    if entry["resolved"]:
        return False
    return time.time() - entry["fetched_at"] > TTL_UNRESOLVED


def _fetch_batch(batch: list[str]) -> None:
    """Runs GET on API for a batch of max 50 tokens, refreshes _CACHE"""
    r = requests.get(TOKENS_URL, params={"token_id": ",".join(batch)}, timeout=10)
    r.raise_for_status()
    now = time.time()

    for row in r.json()["data"]:
        _CACHE[row["token_id"]] = {
            "title": row["title"],
            "outcome": row["outcome"],
            "opposite_token_id": row["opposite_token_id"],
            "event_slug": row["event_slug"],
            "resolved": row["resolved"],
            "final_price": row["final_price"],
            "fetched_at": now,
        }

    # Deal with unknow IDs with placeholders:
    for token_id in batch:
        if token_id not in _CACHE or _CACHE[token_id]["fetched_at"] != now:
            _CACHE[token_id] = {
                "title": None,
                "outcome": None,
                "opposite_token_id": None,
                "event_slug": None,
                "resolved": False,
                "final_price": None,
                "fetched_at": now,
            }


def token_details(token_ids: list[str]) -> dict[str, dict]:
    """Fetches details from list of token IDs using batch processing and cache loading"""

    # Fetch only unseen or stale
    to_fetch = [t for t in set(token_ids) if t not in _CACHE or _is_stale(_CACHE[t])]
    for i in range(0, len(to_fetch), BATCH):
        _fetch_batch(to_fetch[i:i + BATCH])
    return {t: _CACHE[t] for t in token_ids}

