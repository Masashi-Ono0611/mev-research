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

    # Optional: slippage based on median (same scale)
    median_rate = statistics.median(scaled_rates)
    slippages = []
    for r in rows:
        if r["direction"] == "TON->USDT":
            exec_rate = (Decimal(1) / Decimal(r["rate"])) * SCALE
        else:
            exec_rate = Decimal(r["rate"]) * SCALE
        slippages.append(abs(exec_rate - median_rate) / median_rate)

    print("\nSlippage (%)")
    print(f"min: {min(slippages)*100:.4f}")
    print(f"max: {max(slippages)*100:.4f}")
    print(f"median: {statistics.median(slippages)*100:.4f}")
    print(f"mean: {statistics.mean(slippages)*100:.4f}")


if __name__ == "__main__":
    main()
