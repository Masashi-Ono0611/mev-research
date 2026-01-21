import json
import os
import time
import urllib.parse
import statistics
from decimal import Decimal
from pathlib import Path
import requests

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "swaps_sample.ndjson"
SCALE = Decimal(1000)
TON_API_BASE = os.getenv("TON_API_BASE_URL") or os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io"
FETCH_BLOCKS = (os.getenv("MEV_FETCH_BLOCKS") or "true").lower() in ("1", "true", "yes", "on")


def load_rows(path: Path):
    with path.open() as f:
        return [json.loads(line) for line in f]


def extract_primary_lt(row: dict) -> int | None:
    """Prefer notify.in_msg.created_lt (Jetton Notify), fallback to others."""
    # primary choice: Jetton Notify in_msg
    notify_lt = None
    try:
        notify_lt = int((((row.get("notify") or {}).get("in_msg") or {}).get("created_lt")) or 0)
    except Exception:
        notify_lt = None
    if notify_lt:
        return notify_lt

    lts = []
    # swap out_msg
    try:
        lts.append(int((((row.get("swap") or {}).get("out_msg") or {}).get("created_lt")) or 0))
    except Exception:
        pass
    # pay in_msg
    try:
        lts.append(int((((row.get("pay") or {}).get("in_msg") or {}).get("created_lt")) or 0))
    except Exception:
        pass
    # transfer out_msg
    try:
        lts.append(int((((row.get("transfer") or {}).get("out_msg") or {}).get("created_lt")) or 0))
    except Exception:
        pass
    lts = [lt for lt in lts if lt]
    return min(lts) if lts else None


def extract_notify_hash(row: dict) -> str | None:
    return ((row.get("notify") or {}).get("tx_hash")) or None


def fetch_block_id(tx_hash: str) -> dict | None:
    if not tx_hash:
        return None
    url = urllib.parse.urljoin(TON_API_BASE, f"/v2/blockchain/transactions/{tx_hash}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        # tonapi sometimes nests block info under block_id or block
        blk = data.get("block_id") or data.get("block") or {}
        wc = blk.get("workchain") if isinstance(blk, dict) else None
        shard = blk.get("shard") if isinstance(blk, dict) else None
        seqno = blk.get("seqno") if isinstance(blk, dict) else None
        if wc is None or shard is None or seqno is None:
            return None
        return {"workchain": wc, "shard": shard, "seqno": seqno}
    except Exception:
        return None


def block_key(bid: dict | None) -> str | None:
    if not bid:
        return None
    return f"{bid.get('workchain')}:{bid.get('shard')}:{bid.get('seqno')}"


def compute_rates(rows):
    rates = []
    rates_by_dir = {"TON->USDT": [], "USDT->TON": []}
    for r in rows:
        if r["direction"] == "TON->USDT":
            val = Decimal(1) / Decimal(r["rate"])
        else:  # USDT->TON
            val = Decimal(r["rate"])
        rates.append(val * SCALE)
        if r["direction"] in rates_by_dir:
            rates_by_dir[r["direction"]].append(val * SCALE)
        # store per-row scaled rate for later use
        r["rate1000"] = float(val * SCALE)
    return rates, rates_by_dir


def summarize(name: str, values):
    return {
        "name": name,
        "count": len(values),
        "min": float(min(values)),
        "max": float(max(values)),
        "median": float(statistics.median(values)),
        "mean": float(sum(values) / len(values)),
        "stdev": float(statistics.stdev(values)) if len(values) > 1 else 0.0,
    }


def format_stats(label: str, stats: dict) -> str:
    return (
        f"{label}:\n"
        f"  count={stats['count']}\n"
        f"  min={stats['min']:.6f}, max={stats['max']:.6f}\n"
        f"  median={stats['median']:.6f}, mean={stats['mean']:.6f}, stdev={stats['stdev']:.6f}"
    )


def extract_min_out(row: dict) -> Decimal | None:
    """Attempt to extract min_out (as Decimal) from known payload paths."""
    # Path 1: swap.out_msg.decoded_body.dex_payload.swap_body.min_out
    sb1 = (((row.get("swap") or {}).get("out_msg") or {}).get("decoded_body") or {}).get(
        "dex_payload", {}
    ).get("swap_body", {})
    min_out = sb1.get("min_out") if isinstance(sb1, dict) else None

    # Path 2: notify.in_msg.decoded_body.forward_payload.value.value.cross_swap_body.min_out
    if min_out is None:
        sb2 = (
            (((row.get("notify") or {}).get("in_msg") or {}).get("decoded_body") or {})
            .get("forward_payload")
            or {}
        )
        val = sb2.get("value") if isinstance(sb2, dict) else None
        if isinstance(val, dict):
            val = val.get("value")
        if isinstance(val, dict):
            min_out = val.get("cross_swap_body", {}).get("min_out")

    if min_out is None:
        return None
    try:
        return Decimal(min_out)
    except Exception:
        return None


def main():
    rows = load_rows(DATA_PATH)
    # attach primary_lt for ordering (smallest created_lt among component msgs)
    for r in rows:
        r["primary_lt"] = extract_primary_lt(r) or r.get("lt")
    # fetch block info using notify tx_hash (Jetton Notify)
    if FETCH_BLOCKS:
        block_cache = {}
        for r in rows:
            txh = extract_notify_hash(r)
            if txh in block_cache:
                bid = block_cache[txh]
            else:
                bid = fetch_block_id(txh)
                block_cache[txh] = bid
                # small politeness pause to avoid spamming
                time.sleep(0.05)
            r["block_id"] = bid
            r["block_key"] = block_key(bid)
    else:
        for r in rows:
            r["block_id"] = None
            r["block_key"] = None

    # sort by primary_lt, then utime as tiebreaker
    rows.sort(key=lambda x: (x.get("primary_lt", 0), x.get("utime", 0)))
    scaled_rates, rates_by_dir = compute_rates(rows)
    summary = summarize("USDT/TON *1000", scaled_rates)

    # Direction-wise summary first
    print("=== Direction breakdown: USDT/TON decimal-adjusted (scaled by 1000) ===")
    for direction, vals in rates_by_dir.items():
        if not vals:
            continue
        s = summarize(direction, vals)
        print(format_stats(direction, s))

    # Overall summary next
    print("\n=== USDT/TON decimal-adjusted (scaled by 1000) ===")
    for k in ["count", "min", "max", "median", "mean", "stdev"]:
        print(f"{k}: {summary[k]}")

    # min_out vs actual_out comparison
    coverage = []  # entries with min_out present
    missing_min_out = 0
    for r in rows:
        min_out = extract_min_out(r)
        out_amt = None
        try:
            out_amt = Decimal(r["out_amount"])
        except Exception:
            out_amt = None

        if min_out is None or out_amt is None or out_amt == 0:
            missing_min_out += 1
            continue

        try:
            hit_pct = (min_out / out_amt) * 100  # 100% => actual_out == min_out
        except Exception:
            hit_pct = None

        coverage.append(
            {
                "query_id": r.get("query_id"),
                "direction": r.get("direction"),
                "lt": r.get("lt"),
                "out_amount": out_amt,
                "min_out": min_out,
                "hit_pct": float(hit_pct) if hit_pct is not None else None,
            }
        )

    print("\nmin_out coverage (actual_out vs min_out)")
    print(f"with_min_out: {len(coverage)}, missing_or_invalid: {missing_min_out}")

    hit_values = [c["hit_pct"] for c in coverage if c["hit_pct"] is not None]
    if hit_values:
        print(
            f"hit_pct stats (min_out/actual_out * 100): min={min(hit_values):.4f}, "
            f"max={max(hit_values):.4f}, median={statistics.median(hit_values):.4f}, "
            f"mean={statistics.mean(hit_values):.4f}"
        )

        # Top cases closest to min_out (highest hit_pct)
        print("\nTop swaps closest to min_out (desc by hit_pct)")
        for d in sorted(coverage, key=lambda x: (x["hit_pct"] is not None, x["hit_pct"]), reverse=True)[:5]:
            if d["hit_pct"] is None:
                continue
            print(
                f"query_id={d['query_id']}, dir={d['direction']}, lt={d['lt']}, "
                f"hit_pct={d['hit_pct']:.4f}% (min_out={d['min_out']}, actual_out={d['out_amount']})"
            )

    # Same-direction frontrun candidates (victim worse), adjacency-based (previous tx only)
    fr_pairs = []
    for i in range(1, len(rows)):
        fr = rows[i - 1]
        v = rows[i]
        if fr.get("direction") != v.get("direction"):
            continue
        # victim worse: TON->USDT is worse if rate1000 drops; USDT->TON is worse if rate1000 rises
        if v["direction"] == "TON->USDT" and v.get("rate1000") is not None and fr.get("rate1000") is not None:
            if v["rate1000"] < fr["rate1000"]:
                fr_pairs.append((fr, v))
        elif v["direction"] == "USDT->TON" and v.get("rate1000") is not None and fr.get("rate1000") is not None:
            if v["rate1000"] > fr["rate1000"]:
                fr_pairs.append((fr, v))

    print("\nSame-direction frontrun candidates (victim worse, adjacent prev tx, block not considered)")
    print(f"count: {len(fr_pairs)}")
    for fr, v in fr_pairs:
        dt = v.get("utime", 0) - fr.get("utime", 0)
        print(
            f"dt={dt}s | FR qid={fr.get('query_id')} utime={fr.get('utime')} lt={fr.get('lt')} primary_lt={fr.get('primary_lt')} dir={fr.get('direction')} rate1000={float(fr.get('rate1000')):.4f} | "
            f"VICTIM qid={v.get('query_id')} utime={v.get('utime')} lt={v.get('lt')} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f}"
        )

    # Same-block (notify-based block_key) frontrun candidates using primary_lt order
    print("\nSame-block frontrun candidates (notify block, primary_lt order, victim worse; block considered)")
    if FETCH_BLOCKS:
        same_block_pairs = []
        by_block = {}
        for r in rows:
            bk = r.get("block_key")
            if not bk:
                continue
            by_block.setdefault(bk, []).append(r)
        for bk, arr in by_block.items():
            arr.sort(key=lambda x: x.get("primary_lt", 0))
            for i in range(1, len(arr)):
                fr = arr[i - 1]
                v = arr[i]
                if fr.get("direction") != v.get("direction"):
                    continue
                if v["direction"] == "TON->USDT" and v.get("rate1000") is not None and fr.get("rate1000") is not None:
                    if v["rate1000"] < fr["rate1000"]:
                        same_block_pairs.append((bk, fr, v))
                elif v["direction"] == "USDT->TON" and v.get("rate1000") is not None and fr.get("rate1000") is not None:
                    if v["rate1000"] > fr["rate1000"]:
                        same_block_pairs.append((bk, fr, v))

        print(f"count: {len(same_block_pairs)}")
        for bk, fr, v in same_block_pairs:
            dt = v.get("utime", 0) - fr.get("utime", 0)
            print(
                f"block={bk} | dt={dt}s | FR qid={fr.get('query_id')} primary_lt={fr.get('primary_lt')} dir={fr.get('direction')} rate1000={float(fr.get('rate1000')):.4f} | "
                f"VICTIM qid={v.get('query_id')} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f}"
            )
    else:
        print("(block fetch disabled; set MEV_FETCH_BLOCKS=true to enable)")


if __name__ == "__main__":
    main()
