import json
import os
import time
import urllib.parse
import statistics
from decimal import Decimal
from pathlib import Path
from typing import Optional

import requests

DEFAULT_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "swaps_sample.ndjson"
SCALE = Decimal(1000)
TON_API_BASE = os.getenv("TON_API_BASE_URL") or os.getenv("NEXT_PUBLIC_TON_API_BASE_URL") or "https://tonapi.io"
FETCH_BLOCKS = (os.getenv("MEV_FETCH_BLOCKS") or "true").lower() in ("1", "true", "yes", "on")


def load_rows(path: Path):
    with path.open() as f:
        return [json.loads(line) for line in f]


def extract_primary_lt(row: dict) -> Optional[int]:
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


def extract_notify_hash(row: dict) -> Optional[str]:
    return ((row.get("notify") or {}).get("tx_hash")) or None


def fetch_block_id(tx_hash: str) -> Optional[dict]:
    if not tx_hash:
        return None
    def parse_block_str(s: str) -> Optional[dict]:
        try:
            if s.startswith("(") and s.endswith(")"):
                items = s.strip("()").split(",")
                if len(items) == 3:
                    wc = int(items[0])
                    shard = items[1]
                    seqno = int(items[2])
                    return {"workchain": wc, "shard": shard, "seqno": seqno}
        except Exception:
            return None
        return None

    url = urllib.parse.urljoin(TON_API_BASE, f"/v2/blockchain/transactions/{tx_hash}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        blk = data.get("block_id") or data.get("block") or {}
        if isinstance(blk, str):
            parsed = parse_block_str(blk)
            if parsed:
                return parsed
        wc = blk.get("workchain") if isinstance(blk, dict) else None
        shard = blk.get("shard") if isinstance(blk, dict) else None
        seqno = blk.get("seqno") if isinstance(blk, dict) else None
        if wc is None or shard is None or seqno is None:
            return None
        return {"workchain": wc, "shard": shard, "seqno": seqno}
    except Exception:
        return None
    return None


def block_key(bid: Optional[dict]) -> Optional[str]:
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


def sanity_filter(rows):
    """Filter obvious outliers by loose direction-specific ranges.

    Rationale (keep simple):
    - USDT->TON expected rate ~0.0015 (USDT/TON). TON<->TON swaps show rate≈1, so drop anything >0.01 or <1e-6.
    - TON->USDT expected ~600-700 (TON/USDT inverse). Drop anything far outside 10-5000 to avoid dividing errors/extremes.
    """

    filtered = []
    dropped = []
    for r in rows:
        try:
            rate = Decimal(r["rate"])
        except Exception:
            dropped.append((r, "rate_parse_error"))
            continue

        if r.get("direction") == "USDT->TON":
            if rate <= Decimal("1e-6") or rate >= Decimal("0.01"):
                dropped.append((r, "usdt_ton_sanity"))
                continue
        elif r.get("direction") == "TON->USDT":
            if rate <= Decimal("10") or rate >= Decimal("5000"):
                dropped.append((r, "ton_usdt_sanity"))
                continue
        filtered.append(r)

    return filtered, dropped


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


def extract_min_out(row: dict) -> Optional[Decimal]:
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


class Emitter:
    def __init__(self):
        self.lines = []

    def emit(self, msg: str = "") -> None:
        self.lines.append(msg)
        print(msg)

    def save(self, path: Optional[Path]):
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(self.lines), encoding="utf-8")
        self.emit(f"\nSaved summary to {path}")


def build_parser():
    import argparse

    parser = argparse.ArgumentParser(description="MEV rate check")
    parser.add_argument("--data", default=str(DEFAULT_DATA_PATH), help="NDJSON swaps file")
    parser.add_argument("--out", default=None, help="Optional output path for summary text (saved in addition to stdout)")
    parser.add_argument(
        "--enable-cross-block-br",
        action="store_true",
        help="Scan backrun across adjacent blocks (requires MEV_FETCH_BLOCKS=true)",
    )
    parser.add_argument(
        "--block-gap",
        type=int,
        default=1,
        help="Max block seqno gap for cross-block BR scan (same shard). Default 1",
    )
    return parser


def compute_min_out_coverage(rows):
    coverage = []
    missing = 0
    for r in rows:
        min_out = extract_min_out(r)
        try:
            out_amt = Decimal(r["out_amount"])
        except Exception:
            out_amt = None
        if min_out is None or out_amt is None or out_amt == 0:
            missing += 1
            continue
        try:
            hit_pct = (min_out / out_amt) * 100
        except Exception:
            hit_pct = None
        coverage.append({
            "query_id": r.get("query_id"),
            "direction": r.get("direction"),
            "lt": r.get("lt"),
            "out_amount": out_amt,
            "min_out": min_out,
            "hit_pct": float(hit_pct) if hit_pct is not None else None,
        })
    return coverage, missing


def scan_frontrun_adjacent(rows):
    pairs = []
    for i in range(1, len(rows)):
        fr = rows[i - 1]
        v = rows[i]
        if fr.get("direction") != v.get("direction"):
            continue
        if v.get("rate1000") is None or fr.get("rate1000") is None:
            continue
        if v["direction"] == "TON->USDT" and v["rate1000"] < fr["rate1000"]:
            pairs.append((fr, v))
        elif v["direction"] == "USDT->TON" and v["rate1000"] > fr["rate1000"]:
            pairs.append((fr, v))
    return pairs


def scan_backrun_adjacent(rows):
    pairs = []
    for i in range(1, len(rows)):
        v = rows[i - 1]
        b = rows[i]
        if v.get("rate1000") is None or b.get("rate1000") is None:
            continue
        if v.get("direction") == "USDT->TON" and b.get("direction") == "TON->USDT" and b["rate1000"] > v["rate1000"]:
            pairs.append((v, b))
        elif v.get("direction") == "TON->USDT" and b.get("direction") == "USDT->TON" and b["rate1000"] < v["rate1000"]:
            pairs.append((v, b))
    return pairs


def build_block_index(rows):
    by_block = {}
    for r in rows:
        bk = r.get("block_key")
        if not bk:
            continue
        by_block.setdefault(bk, []).append(r)
    for bk in by_block:
        by_block[bk].sort(key=lambda x: x.get("primary_lt", 0))
    return by_block


def scan_same_block_fr(by_block):
    pairs = []
    for bk, arr in by_block.items():
        for i in range(1, len(arr)):
            fr = arr[i - 1]
            v = arr[i]
            if fr.get("direction") != v.get("direction"):
                continue
            if v.get("rate1000") is None or fr.get("rate1000") is None:
                continue
            if v["direction"] == "TON->USDT" and v["rate1000"] < fr["rate1000"]:
                pairs.append((bk, fr, v))
            elif v["direction"] == "USDT->TON" and v["rate1000"] > fr["rate1000"]:
                pairs.append((bk, fr, v))
    return pairs


def scan_same_block_br(by_block):
    pairs = []
    for bk, arr in by_block.items():
        for i in range(1, len(arr)):
            v = arr[i - 1]
            b = arr[i]
            if v.get("rate1000") is None or b.get("rate1000") is None:
                continue
            if v.get("direction") == "USDT->TON" and b.get("direction") == "TON->USDT" and b["rate1000"] > v["rate1000"]:
                pairs.append((bk, v, b))
            elif v.get("direction") == "TON->USDT" and b.get("direction") == "USDT->TON" and b["rate1000"] < v["rate1000"]:
                pairs.append((bk, v, b))
    return pairs


def scan_cross_block_br(rows, block_gap):
    def parse_block_key(bk: str):
        try:
            wc, shard, seq = bk.split(":", 2)
            return int(wc), shard, int(seq)
        except Exception:
            return None

    shard_map = {}
    for r in rows:
        bk = r.get("block_key")
        if not bk:
            continue
        parsed = parse_block_key(bk)
        if not parsed:
            continue
        wc, shard, seq = parsed
        shard_map.setdefault((wc, shard), []).append(r)

    cross_pairs = []
    for (wc, shard), arr in shard_map.items():
        arr.sort(key=lambda x: (x.get("block_id", {}).get("seqno", 0), x.get("primary_lt", 0)))
        for i in range(1, len(arr)):
            v = arr[i - 1]
            b = arr[i]
            bk_v = v.get("block_id") or {}
            bk_b = b.get("block_id") or {}
            seq_v = bk_v.get("seqno")
            seq_b = bk_b.get("seqno")
            if seq_v is None or seq_b is None:
                continue
            if seq_b - seq_v == 0:
                continue  # same-block handled elsewhere
            if seq_b - seq_v > block_gap:
                continue
            if v.get("rate1000") is None or b.get("rate1000") is None:
                continue
            if v.get("direction") == "USDT->TON" and b.get("direction") == "TON->USDT" and b["rate1000"] > v["rate1000"]:
                cross_pairs.append((seq_v, seq_b, v, b))
            elif v.get("direction") == "TON->USDT" and b.get("direction") == "USDT->TON" and b["rate1000"] < v["rate1000"]:
                cross_pairs.append((seq_v, seq_b, v, b))
    return cross_pairs


def main():
    parser = build_parser()
    args = parser.parse_args()

    emitter = Emitter()
    emit = emitter.emit

    rows = load_rows(Path(args.data))
    rows, dropped = sanity_filter(rows)
    emit("== Load & sanity filter ==")
    emit(f"kept={len(rows)}, dropped={len(dropped)}")
    if dropped:
        emit("Dropped samples (up to 5):")
        for r, reason in dropped[:5]:
            emit(
                f"  reason={reason} dir={r.get('direction')} rate={r.get('rate')} qid={r.get('query_id')} in={r.get('in_amount')} out={r.get('out_amount')}"
            )
    # attach primary_lt for ordering (smallest created_lt among component msgs)
    for r in rows:
        r["primary_lt"] = extract_primary_lt(r) or r.get("lt")
    # fetch block info using notify tx_hash (Jetton Notify)
    if FETCH_BLOCKS:
        if not args.enable_cross_block_br:
            emit("(MEV_FETCH_BLOCKS enabled; used for same-block listings)")
        else:
            emit("(MEV_FETCH_BLOCKS enabled; same-block + cross-block BR)")
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
    emit("\n== Direction breakdown: USDT/TON decimal-adjusted (scaled by 1000) ==")
    for direction, vals in rates_by_dir.items():
        if not vals:
            continue
        s = summarize(direction, vals)
        emit(format_stats(direction, s))

    # Overall summary next
    emit("\n== Overall USDT/TON decimal-adjusted (scaled by 1000) ==")
    for k in ["count", "min", "max", "median", "mean", "stdev"]:
        emit(f"{k}: {summary[k]}")

    # min_out vs actual_out comparison
    coverage, missing_min_out = compute_min_out_coverage(rows)
    emit("\n== min_out coverage (actual_out vs min_out) ==")
    emit(f"with_min_out: {len(coverage)}, missing_or_invalid: {missing_min_out}")

    hit_values = [c["hit_pct"] for c in coverage if c["hit_pct"] is not None]
    if hit_values:
        emit(
            f"hit_pct stats (min_out/actual_out * 100): min={min(hit_values):.4f}, "
            f"max={max(hit_values):.4f}, median={statistics.median(hit_values):.4f}, "
            f"mean={statistics.mean(hit_values):.4f}"
        )

        emit("\nTop swaps closest to min_out (desc by hit_pct)")
        for d in sorted(coverage, key=lambda x: (x["hit_pct"] is not None, x["hit_pct"]), reverse=True)[:5]:
            if d["hit_pct"] is None:
                continue
            emit(
                f"query_id={d['query_id']}, dir={d['direction']}, lt={d['lt']}, "
                f"hit_pct={d['hit_pct']:.4f}% (min_out={d['min_out']}, actual_out={d['out_amount']})"
            )

    # Same-direction frontrun candidates (victim worse), adjacency-based (previous tx only)
    fr_pairs = scan_frontrun_adjacent(rows)
    emit("\n== Adjacent frontrun candidates (no block consideration) ==")
    emit(f"count: {len(fr_pairs)}")
    for fr, v in fr_pairs:
        dt = v.get("utime", 0) - fr.get("utime", 0)
        emit(
            f"dt={dt}s | FR qid={fr.get('query_id')} tx={extract_notify_hash(fr)} utime={fr.get('utime')} lt={fr.get('lt')} primary_lt={fr.get('primary_lt')} dir={fr.get('direction')} rate1000={float(fr.get('rate1000')):.4f} | "
            f"VICTIM qid={v.get('query_id')} tx={extract_notify_hash(v)} utime={v.get('utime')} lt={v.get('lt')} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f}"
        )

    # Backrun candidates (victim then opposite-direction tx that benefits from victim move)
    backrun_pairs = scan_backrun_adjacent(rows)
    emit("\n== Adjacent backrun candidates (no block consideration) ==")
    emit(f"count: {len(backrun_pairs)}")
    for v, b in backrun_pairs:
        dt = b.get("utime", 0) - v.get("utime", 0)
        emit(
            f"dt={dt}s | VICTIM qid={v.get('query_id')} tx={extract_notify_hash(v)} utime={v.get('utime')} lt={v.get('lt')} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f} | "
            f"BACKRUN qid={b.get('query_id')} tx={extract_notify_hash(b)} utime={b.get('utime')} lt={b.get('lt')} primary_lt={b.get('primary_lt')} dir={b.get('direction')} rate1000={float(b.get('rate1000')):.4f}"
        )

    # Same-block (notify-based block_key) frontrun candidates using primary_lt order
    emit("\n== Same-block frontrun scan (notify block, primary_lt order) ==")
    if FETCH_BLOCKS:
        by_block = build_block_index(rows)
        emit(f"blocks_with_tx: {len(by_block)}")
        multi = {bk: arr for bk, arr in by_block.items() if len(arr) > 1}
        emit(f"blocks_with_multiple_tx (shown below): {len(multi)}")
        for bk, arr in sorted(multi.items(), key=lambda x: x[0])[:20]:
            emit(
                f"block={bk} count={len(arr)} qids={[a.get('query_id') for a in arr]} "
                f"primary_lt={[a.get('primary_lt') for a in arr]} dirs={[a.get('direction') for a in arr]}"
            )

        same_block_pairs = scan_same_block_fr(by_block)
        emit(f"Same-block FR candidates: count={len(same_block_pairs)}")
        for bk, fr, v in same_block_pairs:
            dt = v.get("utime", 0) - fr.get("utime", 0)
            emit(
                f"block={bk} | dt={dt}s | FR qid={fr.get('query_id')} tx={extract_notify_hash(fr)} primary_lt={fr.get('primary_lt')} dir={fr.get('direction')} rate1000={float(fr.get('rate1000')):.4f} | "
                f"VICTIM qid={v.get('query_id')} tx={extract_notify_hash(v)} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f}"
            )
    else:
        emit("(block fetch disabled; set MEV_FETCH_BLOCKS=true to enable)")

    # Same-block backrun candidates (opposite direction, backrunner benefits)
    emit("\n== Same-block backrun scan (notify block, primary_lt order) ==")
    if FETCH_BLOCKS:
        back_block_pairs = scan_same_block_br(by_block)
        emit(f"Same-block BR candidates: count={len(back_block_pairs)}")
        for bk, v, b in back_block_pairs:
            dt = b.get("utime", 0) - v.get("utime", 0)
            emit(
                f"block={bk} | dt={dt}s | VICTIM qid={v.get('query_id')} tx={extract_notify_hash(v)} primary_lt={v.get('primary_lt')} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f} | "
                f"BACKRUN qid={b.get('query_id')} tx={extract_notify_hash(b)} primary_lt={b.get('primary_lt')} dir={b.get('direction')} rate1000={float(b.get('rate1000')):.4f}"
            )

        # Cross-block backrun scan (adjacent blocks within gap on same shard)
        if args.enable_cross_block_br:
            emit(
                f"\n== Cross-block backrun scan (0 < block gap <= {args.block_gap}, same shard) =="
            )
            cross_pairs = scan_cross_block_br(rows, args.block_gap)
            emit(f"Cross-block BR candidates (0 < block gap <= {args.block_gap}): count={len(cross_pairs)}")
            for seq_v, seq_b, v, b in cross_pairs:
                dt = b.get("utime", 0) - v.get("utime", 0)
                emit(
                    f"shard={v.get('block_key')} seq_gap={seq_b-seq_v} | dt={dt}s | VICTIM qid={v.get('query_id')} tx={extract_notify_hash(v)} seq={seq_v} dir={v.get('direction')} rate1000={float(v.get('rate1000')):.4f} | "
                    f"BACKRUN qid={b.get('query_id')} tx={extract_notify_hash(b)} seq={seq_b} dir={b.get('direction')} rate1000={float(b.get('rate1000')):.4f}"
                )
        else:
            emit("(cross-block backrun scan disabled; use --enable-cross-block-br to enable)")
    else:
        emit("(block fetch disabled; set MEV_FETCH_BLOCKS=true to enable)")

    emitter.save(Path(args.out) if args.out else None)


if __name__ == "__main__":
    main()
