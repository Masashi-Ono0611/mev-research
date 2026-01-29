from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_OUT = "ton-analysis/data/tonco_swaps_latest.ndjson"
DEFAULT_RAW_OUT = "ton-analysis/data/tonco_swaps_tonapi_raw.ndjson"

TONCO_TON_USDT_POOL_ADDR = "EQD25vStEwc-h1QT1qlsYPQwqU5IiOhox5II0C_xsDNpMVo7"

IN_OP_POOLV3_SWAP = "0xa7fb58f8"
OUT_OP_PAY_TO = "0xa1daa96d"

TONCO_TON_WALLET_ADDR = "0:871da9215b14902166f0ea2a16db56278d528108377f8158c5f4ccfdfdd22e17"
TONCO_USDT_WALLET_ADDR = "0:acad45796724b3f00ad42a4311b20667da4be28a43951587a381f73aa9552209"

getcontext().prec = 28


def fetch_page(api_url: str, pool: str, limit: int, api_key: Optional[str], before_lt: Optional[int]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": limit}
    if before_lt:
        params["before_lt"] = before_lt
    headers: Dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = f"{api_url.rstrip('/')}/accounts/{pool}/transactions"
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json().get("transactions", [])


def fetch_pages(
    api_url: str,
    pool: str,
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
        txs = fetch_page(api_url, pool, limit, api_key, cursor)
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

    swap = (parts.get("swap") or {}).get("in_msg") or {}
    src_wallet = norm(((swap.get("decoded_body") or {}).get("source_wallet") or ""))

    if src_wallet == norm(TONCO_TON_WALLET_ADDR):
        return "TON->USDT"
    if src_wallet == norm(TONCO_USDT_WALLET_ADDR):
        return "USDT->TON"

    return "unknown"


def extract_meta(parts: Dict[str, Any]) -> Dict[str, Any]:
    swap = (parts.get("swap") or {}).get("in_msg") or {}
    lt = swap.get("created_lt")
    utime = swap.get("created_at")
    return {"lt": lt, "utime": utime}


def _decimal(val: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return None


def _normalize_addr(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, dict):
        return val.get("address") or val.get("addr") or val.get("value")
    return str(val)


def _extract_pay_to_amounts(pay_to_decoded: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    pay_to = pay_to_decoded.get("pay_to") or {}
    code200 = (pay_to.get("pay_to_code200") or {})
    coins = (code200.get("coinsinfo_cell") or {})

    amount0 = coins.get("amount0")
    jetton0 = _normalize_addr(coins.get("jetton0_address"))
    amount1 = coins.get("amount1")
    jetton1 = _normalize_addr(coins.get("jetton1_address"))

    if amount0 is None or amount1 is None or jetton0 is None or jetton1 is None:
        return None

    return str(amount0), str(jetton0), str(amount1), str(jetton1)


def compute_amounts(parts: Dict[str, Any], direction: str) -> Dict[str, Any]:
    swap = (parts.get("swap") or {}).get("in_msg") or {}
    pay = (parts.get("pay") or {}).get("out_msg") or {}

    swap_decoded = swap.get("decoded_body") or {}
    in_amt = _decimal(((swap_decoded.get("params_cell") or {}).get("amount")))

    out_amt: Optional[Decimal] = None
    pay_decoded = pay.get("decoded_body") or {}
    pay_amounts = _extract_pay_to_amounts(pay_decoded) if pay_decoded else None

    if pay_amounts:
        amount0, jetton0, amount1, jetton1 = pay_amounts
        src_wallet = str((swap_decoded.get("source_wallet") or "")).lower()
        candidates = [
            (amount0, str(jetton0).lower()),
            (amount1, str(jetton1).lower()),
        ]

        non_zero = [(a, j) for a, j in candidates if str(a) not in ("0", "0.0", "")]
        if len(non_zero) == 1:
            out_amt = _decimal(non_zero[0][0])
        else:
            out_by_jetton = [(a, j) for a, j in candidates if j != src_wallet and str(a) not in ("0", "0.0", "")]
            if out_by_jetton:
                out_amt = _decimal(out_by_jetton[0][0])
            elif direction == "USDT->TON":
                out_amt = _decimal(amount0)
            elif direction == "TON->USDT":
                out_amt = _decimal(amount1)

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


def is_successful_swap(direction: str, amounts: Dict[str, Any]) -> bool:
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

        qid = None
        in_op = (in_msg.get("op_code", "") or "").lower()
        if in_op == IN_OP_POOLV3_SWAP:
            qid = str((in_msg.get("decoded_body") or {}).get("query_id", ""))

        if qid in (None, "", "0"):
            for om in out_msgs:
                od = om.get("decoded_body") or {}
                qid = str(od.get("query_id", ""))
                if qid:
                    break

        if not qid or qid == "0":
            continue

        bucket = buckets.setdefault(qid, {"swap": None, "pay": None})

        if in_op == IN_OP_POOLV3_SWAP and bucket.get("swap") is None:
            bucket["swap"] = {"tx_hash": tx.get("hash"), "in_msg": in_msg}

        for om in out_msgs:
            op = (om.get("op_code", "") or "").lower()
            if op == OUT_OP_PAY_TO and bucket.get("pay") is None:
                bucket["pay"] = {"tx_hash": tx.get("hash"), "out_msg": om}

    rows: List[Dict[str, Any]] = []
    for qid, parts in buckets.items():
        if not any(parts.values()):
            continue
        direction = infer_direction(parts)
        if direction == "unknown":
            continue
        meta = extract_meta(parts)
        amounts = compute_amounts(parts, direction)
        if not is_successful_swap(direction, amounts):
            continue
        rows.append({"query_id": qid, "direction": direction, **meta, **amounts, "tx": parts.get("swap", {}).get("tx_hash"), **parts})

    return rows


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch TONCO swaps via tonapi and output NDJSON.")
    parser.add_argument(
        "--api-url",
        default=(os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain",
        help="tonapi base URL",
    )
    parser.add_argument(
        "--pool",
        default=os.getenv("TONCO_POOL_ADDR", TONCO_TON_USDT_POOL_ADDR),
        help="TONCO pool account address",
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
        pool=args.pool,
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
