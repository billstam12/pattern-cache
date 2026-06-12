#!/usr/bin/env python3
"""Detect shape patterns in a real-world time-series CSV.

Writes a ground-truth sidecar in the same format as
``generate_synthetic_csv.py``'s ``<out>.patterns.csv``, so
``scripts/queries/generate_queries.py --patterns ...`` and
``scripts/queries/generate_all.sh`` work unchanged on real datasets.

Output columns:
    from_ms, to_ms, shape, amplitude, segments, measure_id

Shape signatures (mirror SHAPE_SEGMENTS in generate_queries.py):
    triangle    [+,-]
    v_shape     [-,+]
    double_peak [+,-,+,-]
    sawtooth    [+,-,+,-,+,-,+,-]

Pipeline:
  1. Stream the CSV in chunks; aggregate each measure into fixed-width
     time buckets (mean of values in bucket).
  2. Per measure: smooth with a small rolling mean; compute a per-bucket
     local OLS slope over a sliding window (matches how Java's OLSSketch
     fits slope across multiple buckets); sign-encode each bucket as
     '+' / '-' / '0' using ANGLE thresholds — atan(slope) >= rise_angle
     for '+', <= -rise_angle for '-', |atan(slope)| <= flat_angle for
     '0'. Slopes in between are 'ambiguous' and treated as '0' so they
     can't anchor a '+' / '-' segment Java would later reject.
  3. Run-length encode the signs; drop / merge short runs.
  4. Walk the run list and greedily match the longest shape signature
     (sawtooth > double_peak > triangle/v_shape). Each emitted pattern
     covers a non-overlapping span of buckets.

The angle thresholds match SIGN_TO_DEG in scripts/queries/generate_queries.py
(+: 30..89°, -: -89..-30°, 0: -10..10°). Patterns emitted here are exactly
the patterns Java's matcher would classify the same way at query time —
modulo the inevitable difference between the detector's bucket size and
the query's bucket size, since the angle of a slope scales with the bucket
width. Use --bucket-ms close to the typical query bucket size for the dataset.

Usage:
    python3 scripts/data/detect_patterns.py \\
        --csv /opt/exp-data/intel_lab.csv \\
        --time-col datetime \\
        --measures temperature \\
        --bucket-ms 60000 \\
        --out /opt/exp-data/intel_lab.csv.patterns.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# Mirror of SHAPE_SEGMENTS in scripts/queries/generate_queries.py. Kept
# locally so this script can be run without the queries package on path.
# Longest first — the matcher is greedy, so an 8-segment sawtooth wins over
# the 4-segment double_peak that would otherwise consume the same prefix.
SHAPE_SIGNATURES: List[Tuple[str, Tuple[str, ...]]] = [
    ("sawtooth",    ("+", "-", "+", "-", "+", "-", "+", "-")),
    ("double_peak", ("+", "-", "+", "-")),
    ("triangle",    ("+", "-")),
    ("v_shape",     ("-", "+")),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--csv", required=True, help="input CSV")
    p.add_argument("--time-col", default="datetime",
                   help="timestamp column name (default: datetime)")
    p.add_argument("--time-format", default=None,
                   help="explicit strptime format for the time column "
                        "(default: pandas inference)")
    p.add_argument("--measures", required=True,
                   help="comma-separated measure column names to detect over")
    p.add_argument("--bucket-ms", type=int, default=60_000,
                   help="aggregation bucket size in ms (default: 60000 = 1 min)")
    p.add_argument("--chunksize", type=int, default=1_000_000,
                   help="pandas read chunk size (default: 1_000_000)")
    p.add_argument("--smooth-buckets", type=int, default=5,
                   help="rolling mean window in buckets before sign encoding "
                        "(default: 5; 1 disables smoothing)")
    p.add_argument("--slope-window", type=int, default=5,
                   help="sliding-window OLS window in buckets used to estimate "
                        "the local slope at each bucket; must match Java's "
                        "minimum segment length (default: 5)")
    p.add_argument("--rise-angle-deg", type=float, default=30.0,
                   help="bucket is '+' iff atan(slope) >= this many degrees "
                        "(default: 30; matches SIGN_TO_DEG['+'] in "
                        "scripts/queries/generate_queries.py)")
    p.add_argument("--flat-angle-deg", type=float, default=10.0,
                   help="bucket is '0' iff |atan(slope)| <= this many degrees "
                        "(default: 10; matches SIGN_TO_DEG['0'] in "
                        "scripts/queries/generate_queries.py). Buckets between "
                        "flat and rise are 'ambiguous' and treated as '0' for "
                        "RLE so they don't anchor a '+' or '-' segment.")
    p.add_argument("--min-segment-buckets", type=int, default=5,
                   help="runs shorter than this are merged into a neighbour "
                        "(default: 5; mirrors MIN_BUCKETS_PER_SEGMENT in "
                        "generate_queries.py)")
    p.add_argument("--min-amplitude-frac", type=float, default=0.005,
                   help="reject patterns whose (max-min)/range_of_series is "
                        "below this floor (default: 0.005)")
    p.add_argument("--out", required=True, help="output patterns CSV")
    return p.parse_args()


# ----------------------------------------------------------------------------
# Streaming aggregation
# ----------------------------------------------------------------------------


def aggregate_into_buckets(
    csv_path: str, time_col: str, measure_cols: List[str],
    bucket_ms: int, chunksize: int, time_format: Optional[str],
) -> Tuple[int, np.ndarray, Dict[str, np.ndarray]]:
    """Stream `csv_path` and aggregate each measure into fixed-width buckets.

    Returns ``(t0_ms, bucket_ts_ms, {measure: mean_array})`` where bucket_ts_ms
    is the start timestamp (epoch ms) of each bucket and mean_array has NaN for
    buckets that received no samples."""
    usecols = [time_col] + measure_cols
    sums: Dict[str, np.ndarray] = {}
    counts: Dict[str, np.ndarray] = {}
    t0_ms: Optional[int] = None
    max_bucket: int = -1

    reader = pd.read_csv(csv_path, usecols=usecols, chunksize=chunksize)
    for chunk in reader:
        ts = pd.to_datetime(chunk[time_col], format=time_format, utc=True,
                            errors="raise")
        ts_ms = (ts.astype("int64") // 1_000_000).to_numpy()
        if t0_ms is None:
            t0_ms = int(ts_ms.min())
        bucket_idx = ((ts_ms - t0_ms) // bucket_ms).astype(np.int64)
        chunk_max = int(bucket_idx.max())
        if chunk_max > max_bucket:
            new_size = chunk_max + 1
            for m in measure_cols:
                if m not in sums:
                    sums[m] = np.zeros(new_size, dtype=np.float64)
                    counts[m] = np.zeros(new_size, dtype=np.int64)
                else:
                    sums[m] = np.concatenate(
                        [sums[m], np.zeros(new_size - sums[m].size)])
                    counts[m] = np.concatenate(
                        [counts[m], np.zeros(new_size - counts[m].size,
                                             dtype=np.int64)])
            max_bucket = chunk_max
        for m in measure_cols:
            vals = pd.to_numeric(chunk[m], errors="coerce").to_numpy()
            valid = ~np.isnan(vals)
            if not valid.any():
                continue
            bi = bucket_idx[valid]
            vv = vals[valid]
            np.add.at(sums[m], bi, vv)
            np.add.at(counts[m], bi, 1)

    if t0_ms is None or max_bucket < 0:
        raise SystemExit(f"no data read from {csv_path}")

    bucket_ts = t0_ms + np.arange(max_bucket + 1) * bucket_ms
    means: Dict[str, np.ndarray] = {}
    for m in measure_cols:
        s = sums[m]
        c = counts[m].astype(np.float64)
        with np.errstate(invalid="ignore", divide="ignore"):
            mu = np.where(c > 0, s / np.maximum(c, 1), np.nan)
        means[m] = mu
    return t0_ms, bucket_ts, means


# ----------------------------------------------------------------------------
# Sign-encoding and run-length detection
# ----------------------------------------------------------------------------


def _fill_nans(y: np.ndarray) -> np.ndarray:
    """Linear-interpolate over NaN gaps; copy edge values into leading/trailing
    NaNs. Real datasets routinely have sensor dropouts and we don't want those
    to break a run; the alternative is to split runs at every gap, which loses
    real shapes that straddle a dropout."""
    if not np.isnan(y).any():
        return y
    n = y.size
    idx = np.arange(n)
    valid = ~np.isnan(y)
    if not valid.any():
        return y
    y2 = y.copy()
    y2[~valid] = np.interp(idx[~valid], idx[valid], y[valid])
    return y2


def _rolling_mean(y: np.ndarray, w: int) -> np.ndarray:
    if w <= 1:
        return y
    kernel = np.ones(w) / w
    # 'same' length output; edges get partial-window averages via convolution
    # with reflect padding to avoid biasing toward zero.
    pad = w // 2
    padded = np.pad(y, pad, mode="edge")
    return np.convolve(padded, kernel, mode="valid")[:y.size]


def sliding_ols_slope(y: np.ndarray, window: int) -> np.ndarray:
    """Per-bucket local slope estimate via sliding-window OLS, in
    (Δvalue / Δbucket-index) units — same units Java's OLSSketch.calculateAngle
    uses (x is bucket-index, y is the measure mean). Output length equals
    ``y.size``; positions where the window can't be formed are NaN.

    Slope at index i is fit over y[i - W/2 .. i + W/2). Match Java's per-sketch
    OLS so that atan(slope) gives the same angle Java would compute over the
    same buckets at query time."""
    n = y.size
    if window < 2 or n < window:
        return np.full(n, np.nan)
    x = np.arange(window, dtype=np.float64)
    x_centered = x - x.mean()
    x_var = float((x_centered ** 2).sum())
    out = np.full(n, np.nan)
    half = window // 2
    for i in range(n - window + 1):
        seg = y[i:i + window]
        if np.isnan(seg).any():
            continue
        seg_centered = seg - seg.mean()
        out[i + half] = float((x_centered * seg_centered).sum()) / x_var
    return out


def sign_encode(slopes: np.ndarray, rise_angle_deg: float,
                flat_angle_deg: float) -> np.ndarray:
    """Map per-bucket OLS slopes to '+'/'-'/'0' using Java's angle thresholds.

    A bucket is '+' iff atan(slope) >= rise_angle_deg, '-' iff <= -rise_angle_deg,
    '0' iff |atan(slope)| <= flat_angle_deg. Slopes between flat and rise are
    "ambiguous" — Java's matcher would reject them for any sign filter — and
    are folded into '0' here so they break a '+'/'-' run rather than extending
    one. NaN slopes (window underflow) also become '0'."""
    angles = np.degrees(np.arctan(slopes))
    signs = np.where(angles >= rise_angle_deg, "+",
                     np.where(angles <= -rise_angle_deg, "-", "0"))
    # NaN angles already classify as "0" via the chained where (np.nan compares
    # false to both bounds).
    return signs.astype(object)


def run_length_encode(signs: np.ndarray) -> List[Tuple[str, int, int]]:
    """Return list of (sign, start_idx, end_idx_exclusive) runs."""
    n = signs.size
    if n == 0:
        return []
    runs: List[Tuple[str, int, int]] = []
    cur = signs[0]
    start = 0
    for i in range(1, n):
        if signs[i] != cur:
            runs.append((cur, start, i))
            cur = signs[i]
            start = i
    runs.append((cur, start, n))
    return runs


def merge_short_runs(runs: List[Tuple[str, int, int]],
                     min_len: int) -> List[Tuple[str, int, int]]:
    """Drop runs shorter than `min_len` by merging them into the longer of the
    two neighbours (or the only neighbour at an edge). Repeats until stable."""
    if min_len <= 1:
        return runs
    runs = list(runs)
    changed = True
    while changed and len(runs) > 1:
        changed = False
        for i, (s, a, b) in enumerate(runs):
            if b - a >= min_len:
                continue
            # Pick the neighbour that "absorbs" this run.
            left = runs[i - 1] if i > 0 else None
            right = runs[i + 1] if i + 1 < len(runs) else None
            if left is None:
                target_sign = right[0]
            elif right is None:
                target_sign = left[0]
            else:
                target_sign = left[0] if (left[2] - left[1]) >= (right[2] - right[1]) else right[0]
            # Relabel and merge with same-signed neighbour(s).
            new_runs: List[Tuple[str, int, int]] = []
            for j, (sj, aj, bj) in enumerate(runs):
                sj2 = target_sign if j == i else sj
                if new_runs and new_runs[-1][0] == sj2:
                    new_runs[-1] = (sj2, new_runs[-1][1], bj)
                else:
                    new_runs.append((sj2, aj, bj))
            runs = new_runs
            changed = True
            break
    return runs


# ----------------------------------------------------------------------------
# Shape matching
# ----------------------------------------------------------------------------


def match_shapes(
    runs: List[Tuple[str, int, int]],
    bucket_ts_ms: np.ndarray, bucket_ms: int,
    values: np.ndarray, measure_id: str,
    min_amplitude_frac: float,
) -> List[Tuple[int, int, str, float, str, str]]:
    """Greedy left-to-right match: at each cursor try the longest signature
    first; on a hit advance past the matched runs, on a miss advance by one.
    Each output pattern occupies a non-overlapping span of buckets."""
    out: List[Tuple[int, int, str, float, str, str]] = []
    series_range = float(np.nanmax(values) - np.nanmin(values))
    if series_range <= 0 or not np.isfinite(series_range):
        return out
    amp_floor = series_range * min_amplitude_frac

    n = len(runs)
    i = 0
    while i < n:
        matched = False
        for shape_name, sig in SHAPE_SIGNATURES:
            k = len(sig)
            if i + k > n:
                continue
            window = tuple(runs[i + j][0] for j in range(k))
            if window != sig:
                continue
            start_bucket = runs[i][1]
            end_bucket = runs[i + k - 1][2]  # exclusive
            if end_bucket <= start_bucket:
                continue
            seg_vals = values[start_bucket:end_bucket]
            seg_vals = seg_vals[~np.isnan(seg_vals)]
            if seg_vals.size == 0:
                continue
            amplitude = float(seg_vals.max() - seg_vals.min())
            if amplitude < amp_floor:
                # Real shape but too low-amplitude to be a useful anchor; skip
                # and let the next iteration try shorter signatures here.
                continue
            from_ms = int(bucket_ts_ms[start_bucket])
            # to_ms is the end-of-last-bucket boundary, mirroring the synth
            # generator's right-exclusive convention.
            last_bucket = end_bucket - 1
            to_ms = int(bucket_ts_ms[last_bucket]) + bucket_ms
            total_buckets = end_bucket - start_bucket
            seg_field_parts: List[str] = []
            for j in range(k):
                seg_sign, seg_a, seg_b = runs[i + j]
                frac = (seg_b - seg_a) / total_buckets
                seg_field_parts.append(f"{seg_sign}:{frac:.4f}")
            seg_field = ";".join(seg_field_parts)
            out.append((from_ms, to_ms, shape_name, amplitude, seg_field, measure_id))
            i += k
            matched = True
            break
        if not matched:
            i += 1
    return out


# ----------------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------------


def detect_for_measure(
    values: np.ndarray, bucket_ts_ms: np.ndarray, bucket_ms: int,
    measure_id: str, smooth_w: int, slope_window: int,
    rise_angle_deg: float, flat_angle_deg: float,
    min_seg_buckets: int, min_amplitude_frac: float,
) -> Tuple[List[Tuple[int, int, str, float, str, str]], Dict[str, int]]:
    filled = _fill_nans(values)
    smoothed = _rolling_mean(filled, smooth_w)
    slopes = sliding_ols_slope(smoothed, slope_window)
    signs = sign_encode(slopes, rise_angle_deg, flat_angle_deg)
    runs = run_length_encode(signs)
    runs = merge_short_runs(runs, min_seg_buckets)
    # Runs are already in bucket-index space (one sign per bucket).
    patterns = match_shapes(runs, bucket_ts_ms, bucket_ms,
                            values, measure_id, min_amplitude_frac)
    hist: Dict[str, int] = {}
    for p in patterns:
        hist[p[2]] = hist.get(p[2], 0) + 1
    return patterns, hist


def main() -> int:
    args = parse_args()
    measure_cols = [m.strip() for m in args.measures.split(",") if m.strip()]
    if not measure_cols:
        raise SystemExit("--measures must list at least one column")

    print(f"reading {args.csv} in chunks of {args.chunksize:,} rows; "
          f"bucket_ms={args.bucket_ms}", file=sys.stderr)
    t0_ms, bucket_ts_ms, means = aggregate_into_buckets(
        args.csv, args.time_col, measure_cols, args.bucket_ms,
        args.chunksize, args.time_format,
    )
    print(f"aggregated into {bucket_ts_ms.size:,} buckets "
          f"({dt.datetime.utcfromtimestamp(bucket_ts_ms[0] / 1000)} → "
          f"{dt.datetime.utcfromtimestamp(bucket_ts_ms[-1] / 1000)})",
          file=sys.stderr)

    all_patterns: List[Tuple[int, int, str, float, str, str]] = []
    for m in measure_cols:
        patterns, hist = detect_for_measure(
            means[m], bucket_ts_ms, args.bucket_ms, m,
            args.smooth_buckets, args.slope_window,
            args.rise_angle_deg, args.flat_angle_deg,
            args.min_segment_buckets, args.min_amplitude_frac,
        )
        all_patterns.extend(patterns)
        print(f"  {m}: {len(patterns):,} patterns (shapes: {hist})",
              file=sys.stderr)

    all_patterns.sort(key=lambda r: (r[0], r[5]))
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["from_ms", "to_ms", "shape", "amplitude", "segments", "measure_id"])
        for row in all_patterns:
            w.writerow([row[0], row[1], row[2], f"{row[3]:.4f}", row[4], row[5]])
    print(f"wrote {len(all_patterns):,} patterns -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
