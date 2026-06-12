#!/usr/bin/env python3
"""Generate a synthetic time-series CSV with embedded multi-scale patterns.

Output schema (suitable for ``read_csv_auto`` in DuckDB):

    timestamp,id,value

A companion ground-truth file ``<out>.patterns.csv`` is written alongside,
listing every embedded pattern instance with its absolute time range. The
notebook's pattern-detection F1 pipeline can use it directly.

Multi-scale design (relative to the dataset)
--------------------------------------------

Real exploration sessions zoom in and out across many orders of magnitude.
To stress the cache + pattern engine at every zoom level the user can reach,
ramp scales are derived *relative to the dataset's own time span* rather than
fixed absolute durations. This way one ladder works across all dataset sizes
(100k rows or 1B), and pattern density stays meaningful regardless of how
long the dataset covers.

The ladder, level i = 0 .. n_levels-1:

    P_i      = first_scale_fraction * span / 2^i           # duration
    count_i  = ramps_per_window * 2^i                      # # instances embedded
    amp_i    = base_amp * sqrt(P_i / 60s)                  # slope shrinks, visual stays

So at zoom level k (the Markov walk halves the window each zoom-in), the
window has size span / 2^k and the ramps that fit the
``[0.005·W, 0.30·W]`` anchored-pattern band come from a few neighbouring
ladder levels — and on average ``ramps_per_window`` of them are inside.

Levels stop when ``P_i`` falls below ``min_scale_samples * interval`` (a
ramp shorter than ~5 samples isn't slope-detectable at the given sampling
rate), so coarser samplings simply emit fewer levels.

Sizing reference (M1, --chunk 1_000_000):

    rows × measures   disk (CSV)   wall time
    86_400 × 2        ~ 5  MB      ~ 1 s
    10_000_000 × 1    ~ 350 MB     ~ 20 s
    100_000_000 × 1   ~ 3.5 GB     ~ 3 min
    1_000_000_000 × 1 ~ 35 GB      ~ 30 min
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import sys
from typing import List, Tuple

import numpy as np
import pandas as pd


# Each shape is defined by linear anchors (t_frac in [0,1], y_rel in [-1,1])
# plus a slope-sign sequence used by the query generator to emit a multi-segment
# pattern spec. All shapes start and end at y_rel=0 so they overlay cleanly on
# the baseline sinusoid.
SHAPES_SPEC = {
    "triangle":  {"anchors": [(0.0, 0.0), (0.5, 1.0), (1.0, 0.0)],
                  "segments": [("+", 0.50), ("-", 0.50)]},
    "v_shape":   {"anchors": [(0.0, 0.0), (0.5, -1.0), (1.0, 0.0)],
                  "segments": [("-", 0.50), ("+", 0.50)]},
    # M-shape: two peaks with a partial retracement between them.
    "double_peak": {"anchors": [(0.0, 0.0), (0.25, 1.0), (0.5, 0.4),
                                (0.75, 1.0), (1.0, 0.0)],
                    "segments": [("+", 0.25), ("-", 0.25),
                                 ("+", 0.25), ("-", 0.25)]},
    # Four-peak alternating zigzag at equal amplitude.
    "sawtooth": {
        "anchors": [(0.0, 0.0), (1/8, 1.0), (2/8, 0.0), (3/8, 1.0),
                    (4/8, 0.0), (5/8, 1.0), (6/8, 0.0), (7/8, 1.0),
                    (1.0, 0.0)],
        "segments": [("+", 1/8), ("-", 1/8), ("+", 1/8), ("-", 1/8),
                     ("+", 1/8), ("-", 1/8), ("+", 1/8), ("-", 1/8)],
    },
}
SHAPES = list(SHAPES_SPEC.keys())


def _segments_field(shape: str) -> str:
    """Serialize a shape's slope-sign program as ``sign:frac;sign:frac…``."""
    return ";".join(f"{s}:{f:.4f}" for s, f in SHAPES_SPEC[shape]["segments"])

DEFAULT_ROWS_PER_MEASURE = 24 * 3600       # 86_400
DEFAULT_INTERVAL_MS = 1000                 # 1 Hz
DEFAULT_MEASURES = ["synthetic_pat", "synthetic_noise"]
NOISE_AMPLITUDE = 0.05

# Relative-ladder defaults — see module docstring.
DEFAULT_N_LEVELS = 12
DEFAULT_RAMPS_PER_WINDOW = 4
DEFAULT_FIRST_SCALE_FRACTION = 0.10
DEFAULT_MIN_SCALE_SAMPLES = 5
DEFAULT_BASE_AMP = 5.0       # amplitude at the 1-minute reference scale


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--rows", type=int, default=None,
                   help=f"rows per measure (default: {DEFAULT_ROWS_PER_MEASURE:_})")
    p.add_argument("--out", default="/opt/exp-data/synthetic_patterns.csv",
                   help="output CSV path (default: /opt/exp-data/synthetic_patterns.csv)")
    p.add_argument("--start", default="2024-01-01 00:00:00",
                   help="start timestamp YYYY-MM-DD HH:MM:SS")
    p.add_argument("--interval-ms", type=int, default=DEFAULT_INTERVAL_MS,
                   help=f"sampling interval in ms (default: {DEFAULT_INTERVAL_MS} — 1 Hz)")
    p.add_argument("--measures", default=",".join(DEFAULT_MEASURES),
                   help="comma-separated measure names. Patterns are embedded on the FIRST "
                        f"measure; the rest are pure noise (default: {','.join(DEFAULT_MEASURES)})")
    p.add_argument("--seed", type=int, default=0xC0FFEE, help="RNG seed")
    p.add_argument("--chunk", type=int, default=1_000_000,
                   help="rows per write chunk (default: 1_000_000)")
    p.add_argument("--baseline-amp", type=float, default=0.5,
                   help="amplitude of slow background sinusoid (default: 0.5)")
    p.add_argument("--n-levels", type=int, default=DEFAULT_N_LEVELS,
                   help=f"number of ramp scales in the relative ladder "
                        f"(default: {DEFAULT_N_LEVELS}). Levels also stop early if "
                        f"the scale drops below --min-scale-samples * interval.")
    p.add_argument("--ramps-per-window", type=int, default=DEFAULT_RAMPS_PER_WINDOW,
                   help=f"target ramps per query window at each scale's primary "
                        f"zoom level (default: {DEFAULT_RAMPS_PER_WINDOW}). The "
                        f"per-level instance count is this × 2^level.")
    p.add_argument("--first-scale-fraction", type=float, default=DEFAULT_FIRST_SCALE_FRACTION,
                   help=f"duration of the coarsest ramp as a fraction of the "
                        f"dataset span (default: {DEFAULT_FIRST_SCALE_FRACTION}).")
    p.add_argument("--min-scale-samples", type=int, default=DEFAULT_MIN_SCALE_SAMPLES,
                   help=f"minimum ramp length in samples (default: {DEFAULT_MIN_SCALE_SAMPLES}). "
                        f"Levels finer than this are skipped.")
    p.add_argument("--base-amp", type=float, default=DEFAULT_BASE_AMP,
                   help=f"amplitude at the 1-minute reference scale "
                        f"(default: {DEFAULT_BASE_AMP}). Per-level amp scales as sqrt(P/60s).")
    return p.parse_args()


def build_pattern_schedule(rows_per_measure: int, interval_ms: int, rng: np.random.Generator,
                           n_levels: int = DEFAULT_N_LEVELS,
                           ramps_per_window: int = DEFAULT_RAMPS_PER_WINDOW,
                           first_scale_fraction: float = DEFAULT_FIRST_SCALE_FRACTION,
                           min_scale_samples: int = DEFAULT_MIN_SCALE_SAMPLES,
                           base_amp: float = DEFAULT_BASE_AMP,
                           ) -> List[Tuple[int, int, str, float]]:
    """Return a sorted list of (start_row, end_row, shape, amplitude).

    Builds a relative scale ladder anchored on the dataset's own span. Level i:
      duration P_i = first_scale_fraction * span / 2^i
      count    N_i = ramps_per_window * 2^i
      amp_i        = base_amp * sqrt(P_i / 60s)

    Levels stop when P_i drops below min_scale_samples * interval (a ramp
    shorter than that isn't slope-detectable at the current sampling rate).
    """
    span_ms = rows_per_measure * interval_ms
    p0_ms = span_ms * first_scale_fraction
    min_ms = min_scale_samples * interval_ms
    schedule: List[Tuple[int, int, str, float]] = []

    for i in range(n_levels):
        p_ms = p0_ms / (2 ** i)
        if p_ms < min_ms:
            break
        scale_rows = max(min_scale_samples, int(round(p_ms / interval_ms)))
        if scale_rows >= rows_per_measure:
            continue

        instances = ramps_per_window * (2 ** i)
        amp = base_amp * math.sqrt(p_ms / 60_000.0)

        usable = rows_per_measure - scale_rows
        if usable <= 0 or instances <= 0:
            continue

        # Evenly distribute `instances` patterns across the run-up. A small
        # per-instance jitter keeps boundaries from aligning with calendar grids.
        step = usable / instances
        for j in range(instances):
            base = int(j * step)
            jitter = int(rng.integers(0, max(1, int(step * 0.1)) + 1))
            start_row = base + jitter
            end_row = start_row + scale_rows
            if end_row > rows_per_measure:
                continue
            shape = SHAPES[j % len(SHAPES)]
            schedule.append((start_row, end_row, shape, amp))

    schedule.sort(key=lambda e: e[0])
    return schedule


def overlay_for_window(schedule: List[Tuple[int, int, str, float]],
                       global_offset: int, n: int) -> np.ndarray:
    """Compute the additive overlay (length n) for rows [global_offset, +n)
    by applying every pattern in `schedule` that overlaps this window.
    """
    overlay = np.zeros(n, dtype=np.float64)
    win_lo = global_offset
    win_hi = global_offset + n

    # `schedule` is sorted by start_row. Linear scan is fine — the per-chunk
    # overlap count is bounded by the number of scales × max overlap (~8 × ~50
    # at 1M-row chunks), so the loop body runs O(few hundred) times per chunk.
    for start_row, end_row, shape, amp in schedule:
        if end_row <= win_lo:
            continue
        if start_row >= win_hi:
            break  # the rest are even further right

        lo = max(start_row, win_lo)
        hi = min(end_row, win_hi)
        if lo >= hi:
            continue
        local_lo = lo - win_lo
        local_hi = hi - win_lo
        seg_n = local_hi - local_lo

        spec = SHAPES_SPEC.get(shape)
        if spec is None:
            raise ValueError(f"unknown shape: {shape}")
        length = end_row - start_row
        t0 = lo - start_row
        # Sample t in [0, 1) across the visible portion of this pattern.
        t_frac = (np.arange(seg_n) + t0) / max(length, 1)
        xs = [a[0] for a in spec["anchors"]]
        ys = [a[1] for a in spec["anchors"]]
        overlay[local_lo:local_hi] += amp * np.interp(t_frac, xs, ys)

    return overlay


def main() -> int:
    args = parse_args()

    rows_per_measure = args.rows if args.rows is not None else DEFAULT_ROWS_PER_MEASURE
    measures = [m.strip() for m in args.measures.split(",") if m.strip()]
    if not measures:
        raise SystemExit("--measures must list at least one measure name")

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    gt_path = out_path + ".patterns.csv"

    rng = np.random.default_rng(args.seed)
    start_dt = dt.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
    delta = pd.Timedelta(milliseconds=args.interval_ms)
    epoch_ms = int(start_dt.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)

    schedule = build_pattern_schedule(
        rows_per_measure, args.interval_ms, rng,
        n_levels=args.n_levels,
        ramps_per_window=args.ramps_per_window,
        first_scale_fraction=args.first_scale_fraction,
        min_scale_samples=args.min_scale_samples,
        base_amp=args.base_amp,
    )
    by_scale: dict[int, int] = {}
    for s, e, _, _ in schedule:
        by_scale[e - s] = by_scale.get(e - s, 0) + 1

    total_rows = rows_per_measure * len(measures)
    print(f"writing {total_rows:_} rows ({rows_per_measure:_} × {len(measures)} measures) "
          f"-> {out_path}", file=sys.stderr)
    print(f"interval={args.interval_ms} ms; measures={measures}", file=sys.stderr)
    print(f"embedded {len(schedule)} patterns across {len(by_scale)} scales:", file=sys.stderr)
    for scale_rows, count in sorted(by_scale.items()):
        seconds = scale_rows * args.interval_ms / 1000.0
        print(f"    {seconds:>10.1f} s  ({scale_rows:_} rows)  × {count}", file=sys.stderr)
    print(f"ground truth -> {gt_path}", file=sys.stderr)

    # Emit ground truth once up front — the schedule already lists every pattern.
    with open(gt_path, "w", newline="") as gt_fh:
        gt_writer = csv.writer(gt_fh)
        gt_writer.writerow(["from_ms", "to_ms", "shape", "amplitude", "segments", "measure_id"])
        for start_row, end_row, shape, amp in schedule:
            from_ms = epoch_ms + int(start_row * args.interval_ms)
            to_ms   = epoch_ms + int(end_row   * args.interval_ms)
            gt_writer.writerow([from_ms, to_ms, shape, f"{amp:.2f}",
                                _segments_field(shape), measures[0]])

    with open(out_path, "w") as fh:
        fh.write("timestamp,id,value\n")

        emitted = 0
        baseline_period_rows = max(rows_per_measure // 10, 100)
        while emitted < rows_per_measure:
            n = min(args.chunk, rows_per_measure - emitted)

            # date_range returns a DatetimeIndex (which has .strftime); the
            # earlier Timestamp + ndarray*delta yields a bare ndarray of
            # datetime64, which doesn't.
            ts_index = pd.date_range(
                start=pd.Timestamp(start_dt) + emitted * delta,
                periods=n,
                freq=delta,
            )
            ts_str = ts_index.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3].to_numpy()

            phase = 2 * math.pi * (emitted + np.arange(n)) / baseline_period_rows
            baseline = args.baseline_amp * np.sin(phase)
            overlay = overlay_for_window(schedule, emitted, n)

            rows = n * len(measures)
            out_ts = np.repeat(ts_str, len(measures))
            out_id = np.tile(measures, n)
            out_val = np.empty(rows, dtype=np.float64)

            for i in range(len(measures)):
                if i == 0:
                    vals = baseline + overlay + rng.normal(0.0, NOISE_AMPLITUDE, size=n)
                else:
                    vals = rng.normal(0.0, NOISE_AMPLITUDE, size=n)
                out_val[i :: len(measures)] = vals

            df = pd.DataFrame({"timestamp": out_ts, "id": out_id, "value": out_val})
            df.to_csv(fh, index=False, header=False, float_format="%.6f")

            emitted += n
            if emitted % (10 * args.chunk) == 0 or emitted == rows_per_measure:
                pct = 100.0 * emitted / rows_per_measure
                print(f"  {emitted:_} / {rows_per_measure:_} ({pct:5.1f}%)", file=sys.stderr)

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"done. {out_path}: {size_mb:.1f} MB; {len(schedule)} patterns embedded.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
