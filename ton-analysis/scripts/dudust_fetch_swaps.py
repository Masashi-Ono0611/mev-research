"""
DeDust Classic swap fetcher via tonapi.io (bundled by query_id).
- Paging with limit (default 50) is supported; use --pages / --before-lt.
- Mergesort Notify / Swap / PayTo / PayoutFromPool are bundled by query_id with direction/in/out/rate to NDJSON.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Dict, List, Optional

import requests

DEFAULT_OUT = "ton-analysis/data/dudust_swaps_latest.ndjson"
DEFAULT_RAW_OUT = "ton-analysis/data/dudust_swaps_tonapi_raw.ndjson"
DEDUST_TON_USDT_POOL_ADDR = "EQA-X_yo3fzzbDbJ_0bzFWKqtRuZFIRa1sJsveZJ1YpViO3r"

getcontext().prec = 28

IN_OP_SWAP_EXTERNAL = "0x61ee542d"
OUT_OP_PAYOUT_FROM_POOL = "0xad4eb6f5"
OUT_OP_DEDUST_SWAP = "0x9c610de3"

# Wallets to decide direction (addresses observed in tonapi responses)
MERGESORT_ADDR = "0:dae153a74d894bbc32748198cd626e4f5df4a69ad2fa56ce80fc2644b5708d20"
DEDUST_USDT_VAULT_ADDR = "0:18aa8e2eed51747dae033c079b93883d941cad8f65459f2ee9cd7474b6b8ed5d"


def fetch_page(api_url: str, router: str, limit: int, api_key: Optional[str], before_lt: Optional[int]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": limit}
    if before_lt:
        params["before_lt"] = before_lt
    headers: Dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = f"{api_url.rstrip('/')}/accounts/{router}/transactions"
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json().get("transactions", [])


def fetch_pages(
    api_url: str,
    router: str,
    limit: int,
    pages: int,
    api_key: Optional[str],
    before_lt: Optional[int],
    cutoff_utime: Optional[int],
    sleep_secs: float = 0.0,
) -> List[Dict[str, Any]]:
    all_txs: List[Dict[str, Any]] = []
    cursor = before_lt
    for _ in range(max(1, pages)):
        txs = fetch_page(api_url, router, limit, api_key, cursor)
        if not txs:
            break
        all_txs.extend(txs)
        if cutoff_utime:
            try:
                if min(int(t.get("utime", 0)) for t in txs if t.get("utime") is not None) < cutoff_utime:
                    break
            except ValueError:
                pass
        if len(txs) < limit:
            break
        # advance cursor to fetch older txs
        try:
            min_lt = min(int(t.get("lt", 0)) for t in txs if t.get("lt") is not None)
        except ValueError:
            break
        cursor = min_lt - 1
        if sleep_secs > 0:
            time.sleep(sleep_secs)
    return all_txs


def infer_direction(parts: Dict[str, Any]) -> str:
    def norm(addr: str) -> str:
        return (addr or "").lower()

    notify = parts.get("notify") or {}
    n_src = norm(((notify.get("in_msg") or {}).get("source") or {}).get("address", ""))

    transfer = parts.get("transfer") or {}
    t_dest = norm(((transfer.get("out_msg") or {}).get("destination") or {}).get("address", ""))

    if n_src == norm(MERGESORT_ADDR) and t_dest == norm(DEDUST_USDT_VAULT_ADDR):
        return "TON->USDT"
    if n_src == norm(DEDUST_USDT_VAULT_ADDR) and t_dest == norm(MERGESORT_ADDR):
        return "USDT->TON"

    return "unknown"


def extract_meta(parts: Dict[str, Any]) -> Dict[str, Any]:
    notify = parts.get("notify") or {}

    in_msg = (notify.get("in_msg") or {}) if notify else {}
    lt = (in_msg.get("created_lt"))
    utime = (in_msg.get("created_at"))
    return {"lt": lt, "utime": utime}


def compute_amounts(parts: Dict[str, Any], direction: str) -> Dict[str, Any]:
    def d(val: Any) -> Optional[Decimal]:
        try:
            return Decimal(str(val))
        except (InvalidOperation, TypeError):
            return None

    notify = (parts.get("notify") or {}).get("in_msg") or {}
    transfer = (parts.get("transfer") or {}).get("out_msg") or {}

    in_amt = d(((notify.get("decoded_body") or {}).get("amount")))
    out_amt = d(((transfer.get("decoded_body") or {}).get("amount")))

    rate = None
    if in_amt and out_amt and in_amt != 0:
        try:
            rate = (out_amt / in_amt).quantize(Decimal("1.000000000000000000"))
        except InvalidOperation:
            rate = None

    return {
        "in_amount": str(in_amt) if in_amt is not None else None,
        "out_amount": str(out_amt) if out_amt is not None else None,
        "rate": str(rate) if rate is not None else None,
    }


def is_successful_swap(parts: Dict[str, Any], direction: str, amounts: Dict[str, Any]) -> bool:
    out_amt_str = amounts.get("out_amount")
    in_amt_str = amounts.get("in_amount")
    if out_amt_str in (None, "0"):
        return False
    if in_amt_str in (None, "0"):
        return False
    return direction != "unknown"


def build_bundles(txs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for tx in txs:
        in_msg = tx.get("in_msg") or {}
        out_msgs = tx.get("out_msgs") or []

        in_op = (in_msg.get("op_code", "") or "").lower()
        role = None
        qid = None
        if in_op == IN_OP_SWAP_EXTERNAL:
            qid = str((in_msg.get("decoded_body") or {}).get("query_id", ""))
            role = "notify"
        if qid in (None, "", "0"):
            for om in out_msgs:
                od = om.get("decoded_body") or {}
                qid = str(od.get("query_id", ""))
                if qid:
                    break
        if not qid or qid == "0":
            continue

        bucket = buckets.setdefault(qid, {"notify": None, "swap": None, "pay": None, "transfer": None})

        if role == "notify":
            bucket["notify"] = {"tx_hash": tx.get("hash"), "in_msg": in_msg}

        for om in out_msgs:
            op = (om.get("op_code", "") or "").lower()
            if op == OUT_OP_PAYOUT_FROM_POOL and bucket.get("transfer") is None:
                bucket["transfer"] = {"tx_hash": tx.get("hash"), "out_msg": om}
            if op == OUT_OP_DEDUST_SWAP and bucket.get("swap") is None:
                bucket["swap"] = {"tx_hash": tx.get("hash"), "out_msg": om}

    rows: List[Dict[str, Any]] = []
    for qid, parts in buckets.items():
        if not any(parts.values()):
            continue
        direction = infer_direction(parts)
        if direction == "unknown":
            continue
        meta = extract_meta(parts)
        amounts = compute_amounts(parts, direction)
        if not is_successful_swap(parts, direction, amounts):
            continue
        rows.append({"query_id": qid, "direction": direction, **meta, **amounts, **parts})

    return rows


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch DeDust Classic swaps via tonapi and output NDJSON.")
    parser.add_argument(
        "--api-url",
        default=(os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain",
        help="tonapi base URL",
    )
    parser.add_argument(
        "--router",
        default=os.getenv("TON_ROUTER", DEDUST_TON_USDT_POOL_ADDR),
        help="Pool account address",
    )
    parser.add_argument("--limit", type=int, default=50, help="Page size (tonapi limit)")
    parser.add_argument("--pages", type=int, default=20, help="How many pages to fetch (pagination backward by lt)")
    parser.add_argument("--before-lt", type=int, default=None, help="Optional before_lt for pagination anchor")
    parser.add_argument("--max-age-mins", type=int, default=None, help="Stop when tx utime older than now - max_age_min")
    parser.add_argument("--sleep-secs", type=float, default=0.0, help="Optional sleep seconds between page fetches")
    parser.add_argument("--out", default=DEFAULT_OUT, help="NDJSON output path")
    parser.add_argument("--raw-out", default=DEFAULT_RAW_OUT, help="Optional: save raw tonapi txs to NDJSON")
    parser.add_argument(
        "--api-key",
        default=os.getenv("NEXT_PUBLIC_TON_API_KEY") or os.getenv("TON_API_KEY_MAINNET"),
        help="tonapi API key (optional)",
    )
    args = parser.parse_args(argv)

    cutoff_utime = None
    if args.max_age_mins:
        cutoff_utime = int(time.time()) - args.max_age_mins * 60

    txs = fetch_pages(
        api_url=args.api_url,
        router=args.router,
        limit=args.limit,
        pages=args.pages,
        api_key=args.api_key,
        before_lt=args.before_lt,
        cutoff_utime=cutoff_utime,
        sleep_secs=args.sleep_secs,
    )
    rows = build_bundles(txs)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    if args.raw_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.raw_out)), exist_ok=True)
        with open(args.raw_out, "w", encoding="utf-8") as f:
            for tx in txs:
                f.write(json.dumps(tx, ensure_ascii=False) + "\n")

    print(f"fetched {len(rows)} query_id bundles -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
