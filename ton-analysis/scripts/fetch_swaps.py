"""
STON.fi swap fetcher via tonapi.io for a specific router account.
- Pulls account transactions, matches In/Out by query_id, and outputs NDJSON swap records.
- Direction is inferred by Jetton Notify source wallet (pTON vs USDT jetton wallets).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import time

import requests

DEFAULT_OUT = "ton-analysis/data/swaps_24h.ndjson"
TON_ROUTER = "EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3"

# Wallets to decide direction
PTON_WALLET = "0:922d627d7d8edbd00e4e23bdb0c54a76ee5e1f46573a1af4417857fa3e23e91f"  # Proxy TON pTON
USDT_WALLET = "0:9220c181a6cfeacd11b7b8f62138df1bb9cc82b6ed2661d2f5faee204b3efb20"  # Tether USD


@dataclass
class SwapLog:
    tx_hash: str
    lt: int
    utime: int
    direction: str  # "TON->USDT" or "USDT->TON" or "unknown"
    query_id: str
    sender: str
    in_amount: str
    out_amount: str
    raw: Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(
            {
                "tx_hash": self.tx_hash,
                "lt": self.lt,
                "utime": self.utime,
                "direction": self.direction,
                "query_id": self.query_id,
                "sender": self.sender,
                "in_amount": self.in_amount,
                "out_amount": self.out_amount,
                "raw": self.raw,
            },
            ensure_ascii=False,
        )


def fetch_page(api_url: str, router: str, limit: int, before_lt: Optional[int], api_key: Optional[str]) -> Dict[str, Any]:
    params: Dict[str, Any] = {"limit": limit}
    if before_lt:
        params["before_lt"] = before_lt
    headers: Dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = f"{api_url.rstrip('/')}/accounts/{router}/transactions"
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def infer_direction(from_wallet: str) -> str:
    if from_wallet.lower() == PTON_WALLET.lower():
        return "TON->USDT"
    if from_wallet.lower() == USDT_WALLET.lower():
        return "USDT->TON"
    return "unknown"


def parse_swaps(tx: Dict[str, Any]) -> Iterable[SwapLog]:
    tx_hash = tx.get("hash", "")
    lt = int(tx.get("lt", 0))
    utime = int(tx.get("utime", 0))
    in_msg = tx.get("in_msg") or {}
    in_decoded = in_msg.get("decoded_body") or {}
    query_id = str(in_decoded.get("query_id", ""))
    in_amount = str(in_decoded.get("amount", ""))
    if not in_amount:
        # fallback to TON value when Jetton amount not present (e.g., pay_to_v2 paths)
        val = in_msg.get("value")
        if val is not None:
            in_amount = str(val)
    sender = in_decoded.get("sender") or (in_msg.get("source") or {}).get("address", "")

    # Determine direction by source wallet
    source_addr = (in_msg.get("source") or {}).get("address", "")
    direction = infer_direction(source_addr)

    # Find matching out_msg with same query_id
    out_msgs = tx.get("out_msgs") or []
    out_amount = ""
    for om in out_msgs:
        od = om.get("decoded_body") or {}
        if str(od.get("query_id", "")) == query_id:
            out_amount = str(od.get("amount", ""))
            break

    if not query_id or not in_amount or not out_amount:
        return []

    yield SwapLog(
        tx_hash=tx_hash,
        lt=lt,
        utime=utime,
        direction=direction,
        query_id=query_id,
        sender=sender or "",
        in_amount=in_amount,
        out_amount=out_amount,
        raw=tx,
    )


def fetch_all(api_url: str, router: str, limit: int, api_key: Optional[str], before_lt: Optional[int]) -> List[SwapLog]:
    payload = fetch_page(api_url, router, limit, before_lt, api_key)
    txs = payload.get("transactions", [])
    swaps: List[SwapLog] = []
    for tx in txs:
        swaps.extend(parse_swaps(tx))
    return swaps


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch STON.fi swaps via tonapi and output NDJSON.")
    parser.add_argument(
        "--api-url",
        default=(os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io") + "/v2/blockchain",
        help="tonapi base URL",
    )
    parser.add_argument("--router", default=os.getenv("TON_ROUTER", TON_ROUTER), help="Router account address")
    parser.add_argument("--limit", type=int, default=30, help="Page size (tonapi limit)")
    parser.add_argument("--before-lt", type=int, default=None, help="Optional before_lt for pagination anchor")
    parser.add_argument("--out", default=DEFAULT_OUT, help="NDJSON output path")
    parser.add_argument(
        "--api-key",
        default=os.getenv("NEXT_PUBLIC_TON_API_KEY") or os.getenv("TON_API_KEY_MAINNET"),
        help="tonapi API key (optional)",
    )
    args = parser.parse_args(argv)

    swaps = fetch_all(
        api_url=args.api_url,
        router=args.router,
        limit=args.limit,
        api_key=args.api_key,
        before_lt=args.before_lt,
    )

    with open(args.out, "w", encoding="utf-8") as f:
        for s in swaps:
            f.write(s.to_json() + "\n")

    print(f"fetched {len(swaps)} swaps -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
