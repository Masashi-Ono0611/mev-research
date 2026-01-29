"""
STON.fi swap fetcher via tonapi.io (bundled by query_id).
- Paging with limit (default 30) is supported; use --pages / --before-lt.
- Jetton Notify / SwapV2 / PayToV2 / Jetton Transfer are bundled by query_id with direction/in/out/rate to NDJSON.
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

DEFAULT_OUT = "ton-analysis/data/stonfi_swaps_latest.ndjson"
DEFAULT_RAW_OUT = "ton-analysis/data/stonfi_swaps_tonapi_raw.ndjson"
TON_ROUTER = "EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3"

# precision for rate
getcontext().prec = 28

# opcodes
IN_OP_NOTIFY = "0x7362d09c"
IN_OP_PAY_V2 = "0x657b54f5"
OUT_OP_SWAP_V2 = "0x6664de2a"
OUT_OP_TRANSFER = "0x0f8a7ea5"

# Wallets to decide direction
USDT_WALLET = "0:922d627d7d8edbd00e4e23bdb0c54a76ee5e1f46573a1af4417857fa3e23e91f"
PTON_WALLET = "0:9220c181a6cfeacd11b7b8f62138df1bb9cc82b6ed2661d2f5faee204b3efb20"


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
    return all_txs


def infer_direction(parts: Dict[str, Any]) -> str:
    """Infer swap direction using swap.dex_payload.token_wallet1 only.

    Observed data shows all rows can be classified by this field; other fallbacks unused.
    """

    swap_wallet1 = (((parts.get("swap") or {}).get("out_msg") or {}).get("decoded_body") or {}).get(
        "dex_payload", {}
    ).get("token_wallet1")

    if swap_wallet1 == USDT_WALLET:
        return "TON->USDT"
    if swap_wallet1 == PTON_WALLET:
        return "USDT->TON"

    return "unknown"


def is_usdt_ton_pool(parts: Dict[str, Any]) -> bool:
    """Check pool tokens are exactly USDT and PTON.

    Uses pay.additional_info.{token0_address, token1_address}. Observed data always includes pay; if absent,
    conservatively return False to avoid leaking other pools.
    """

    pay_decoded = ((parts.get("pay") or {}).get("in_msg") or {}).get("decoded_body") or {}
    add_info = (pay_decoded.get("additional_info") or {}) if pay_decoded else {}

    token0 = add_info.get("token0_address")
    token1 = add_info.get("token1_address")

    if not token0 or not token1:
        return False

    tokens = {token0, token1}
    return tokens == {USDT_WALLET, PTON_WALLET}


def extract_meta(parts: Dict[str, Any]) -> Dict[str, Any]:
    notify = parts.get("notify") or {}
    meta = {
        "lt": (notify.get("in_msg") or {}).get("created_lt"),
        "utime": (notify.get("in_msg") or {}).get("created_at"),
    }
    return meta


def compute_amounts(parts: Dict[str, Any], direction: str) -> Dict[str, Any]:
    def d(val: Any) -> Optional[Decimal]:
        try:
            return Decimal(str(val))
        except (InvalidOperation, TypeError):
            return None

    notify = (parts.get("notify") or {}).get("in_msg") or {}
    swap = (parts.get("swap") or {}).get("out_msg") or {}
    pay = (parts.get("pay") or {}).get("in_msg") or {}
    transfer = (parts.get("transfer") or {}).get("out_msg") or {}

    in_amt = None
    out_amt = None

    if direction == "TON->USDT":
        # Notify amount is the authoritative TON input; fallback is not used (observed unused).
        in_amt = d(((notify.get("decoded_body") or {}).get("amount")))
        out_amt = d(((transfer.get("decoded_body") or {}).get("amount")))
    elif direction == "USDT->TON":
        # For USDT input, jetton_notify.amount is the authoritative source (fallbacks unused in recent data).
        in_amt = d(((notify.get("decoded_body") or {}).get("amount")))

        # Output uses transfer amount only.
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
    """Determine swap success / filter refunds.

    Rules:
    - If pay is present: require exit_code == 3326308581 and additional_info output > 0.
    - Output must be signaled by transfer amount or pay.additional_info.*_out.
    """

    transfer_decoded = ((parts.get("transfer") or {}).get("out_msg") or {}).get("decoded_body") or {}
    transfer_amount = transfer_decoded.get("amount")

    pay_decoded = ((parts.get("pay") or {}).get("in_msg") or {}).get("decoded_body") or {}
    add_info = (pay_decoded.get("additional_info") or {}) if pay_decoded else {}

    if pay_decoded:
        exit_code = pay_decoded.get("exit_code")
        if exit_code != 3326308581:
            return False

    out_from_pay = None
    if direction == "TON->USDT":
        out_from_pay = add_info.get("amount1_out")
    else:
        out_from_pay = add_info.get("amount0_out")

    output_ok = False
    if transfer_amount not in (None, "0"):
        output_ok = True
    if out_from_pay not in (None, "0"):
        output_ok = True
    if not output_ok:
        return False

    return True


def build_bundles(txs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for tx in txs:
        in_msg = tx.get("in_msg") or {}
        out_msgs = tx.get("out_msgs") or []

        in_op = (in_msg.get("op_code", "") or "").lower()
        role = None
        qid = None
        if in_op in {IN_OP_NOTIFY, IN_OP_PAY_V2}:
            qid = str((in_msg.get("decoded_body") or {}).get("query_id", ""))
            role = "notify" if in_op == IN_OP_NOTIFY else "pay"
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
                if (om.get("op_code", "") or "").lower() == OUT_OP_SWAP_V2:
                    bucket["swap"] = {"tx_hash": tx.get("hash"), "out_msg": om}
        elif role == "pay":
            bucket["pay"] = {"tx_hash": tx.get("hash"), "in_msg": in_msg}
            for om in out_msgs:
                if (om.get("op_code", "") or "").lower() == OUT_OP_TRANSFER:
                    bucket["transfer"] = {"tx_hash": tx.get("hash"), "out_msg": om}
        else:
            for om in out_msgs:
                op = (om.get("op_code", "") or "").lower()
                if op == OUT_OP_SWAP_V2 and bucket.get("swap") is None:
                    bucket["swap"] = {"tx_hash": tx.get("hash"), "out_msg": om}
                if op == OUT_OP_TRANSFER and bucket.get("transfer") is None:
                    bucket["transfer"] = {"tx_hash": tx.get("hash"), "out_msg": om}

    rows: List[Dict[str, Any]] = []
    for qid, parts in buckets.items():
        if not any(parts.values()):
            continue
        direction = infer_direction(parts)
        if direction == "unknown":
            continue
        if not is_usdt_ton_pool(parts):
            continue
        meta = extract_meta(parts)
        amounts = compute_amounts(parts, direction)

        # Filter out refunds / failed swaps
        if not is_successful_swap(parts, direction, amounts):
            continue

        rows.append({"query_id": qid, "direction": direction, **meta, **amounts, **parts})

    return rows


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch STON.fi swaps via tonapi and output NDJSON.")
    parser.add_argument(
        "--api-url",
        default=(os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain",
        help="tonapi base URL",
    )
    parser.add_argument("--router", default=os.getenv("TON_ROUTER", TON_ROUTER), help="Router account address")
    parser.add_argument("--limit", type=int, default=50, help="Page size (tonapi limit)")
    parser.add_argument("--pages", type=int, default=20, help="How many pages to fetch (pagination backward by lt)")
    parser.add_argument("--before-lt", type=int, default=None, help="Optional before_lt for pagination anchor")
    parser.add_argument("--max-age-mins", type=int, default=None, help="Stop when tx utime older than now - max_age_min")
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
    )

    # Save raw tonapi transactions if requested
    if args.raw_out:
        raw_path = os.path.abspath(args.raw_out)
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, "w", encoding="utf-8") as f_raw:
            for tx in txs:
                f_raw.write(json.dumps(tx, ensure_ascii=False) + "\n")
        print(f"saved raw tonapi txs: {len(txs)} -> {raw_path}")

    rows = build_bundles(txs)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"fetched {len(rows)} query_id bundles -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
