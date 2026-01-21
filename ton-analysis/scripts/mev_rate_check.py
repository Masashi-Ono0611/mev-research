import json
import statistics
from decimal import Decimal
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "swaps_sample.ndjson"
SCALE = Decimal(1000)


def load_rows(path: Path):
    with path.open() as f:
        return [json.loads(line) for line in f]


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


if __name__ == "__main__":
    main()
