#!/usr/bin/env python3
"""Generate a query sequence (visual + pattern) for a dataset, in one step.

Takes a single positional argument — the dataset name — and locates all
other inputs/outputs by convention:

    ${DATA_DIR}/<dataset>.csv                  (required: raw time series)
    ${DATA_DIR}/<dataset>.csv.patterns.csv     (required: ground-truth patterns)
    queries/<dataset>/queries.txt              (output)

``DATA_DIR`` defaults to ``/opt/exp-data``.

The script crashes if either input file is missing.

The output line format matches what the Java harness reads at experiment time:

    from_ms,to_ms,from_str,to_str,measure,width,height,accuracy,type,label[,...]

  * ``type=visual``  → ``label ∈ {initial, pan, zoomIn, zoomOut, resize}``
  * ``type=pattern`` → ``label=custom`` followed by ``time_unit, pattern_spec``

Pattern queries are anchored to ground-truth shapes from the patterns CSV
(see ``scripts/data/detect_patterns.py`` for the real-dataset detector or
``scripts/data/generate_synthetic_csv.py`` for synthetic data). When no GT
shape fits the current viewport, the pattern slot falls through to a visual
query instead.

Run:

        python3 scripts/queries/generate_queries.py synth_10y_1m
        python3 scripts/queries/generate_queries.py intel_lab --seq-count 100
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import random
import sys
from typing import List, Optional, Tuple


# Restricted to calendar-divisor units (DateTimeUtil.CALENDAR_INTERVALS):
# minutes must divide 60; hours must divide 24 — otherwise the Java side
# snaps the outer bucket down (e.g. 45 min → 30 min) and the query reads
# differently from what's emitted.
TIME_UNITS_MS: List[Tuple[int, str]] = [
    (1_000,            "1 Seconds"),
    (2_000,            "2 Seconds"),
    (3_000,            "3 Seconds"),
    (5_000,            "5 Seconds"),
    (6_000,            "6 Seconds"),
    (10_000,           "10 Seconds"),
    (12_000,           "12 Seconds"),
    (15_000,           "15 Seconds"),
    (20_000,           "20 Seconds"),
    (30_000,           "30 Seconds"),
    (60_000,           "1 Minutes"),
    (2 * 60_000,       "2 Minutes"),
    (3 * 60_000,       "3 Minutes"),
    (5 * 60_000,       "5 Minutes"),
    (6 * 60_000,       "6 Minutes"),
    (10 * 60_000,      "10 Minutes"),
    (12 * 60_000,      "12 Minutes"),
    (15 * 60_000,      "15 Minutes"),
    (20 * 60_000,      "20 Minutes"),
    (30 * 60_000,      "30 Minutes"),
    (3_600_000,        "1 Hours"),
    (2 * 3_600_000,    "2 Hours"),
    (3 * 3_600_000,    "3 Hours"),
    (4 * 3_600_000,    "4 Hours"),
    (6 * 3_600_000,    "6 Hours"),
    (8 * 3_600_000,    "8 Hours"),
    (12 * 3_600_000,   "12 Hours"),
    (24 * 3_600_000,   "1 Days"),
]

# Pattern-query placement constraints (matches interleave_pattern_queries.py).
MIN_PATTERN_FRACTION = 0.005
MAX_PATTERN_FRACTION = 0.10
MIN_BUCKETS_IN_WINDOW = 5
MAX_BUCKETS_IN_WINDOW = 50_000

# Viewport-natural bucket count: the unit that gives roughly this many buckets
# across the current window is the "natural" granularity for that zoom level.
# Pattern queries then sample 2 to STEPS_BELOW_MAX TIME_UNITS_MS positions
# finer than that, so consecutive queries on similar viewports get similar
# (rather than wildly disparate) bucket sizes.
TARGET_VIEWPORT_BUCKETS = 1000
STEPS_BELOW_MIN = 2
STEPS_BELOW_MAX = 8

# Slope-sign → filter angle range used when emitting multi-segment pattern specs.
# Wide enough to absorb the per-level amplitude jitter in the synthetic generator
# while still rejecting the opposite slope direction.
SIGN_TO_DEG = {
    "+": (30, 89),
    "-": (-89, -30),
    "0": (-10, 10),
}

# Slope-sign program per shape — kept in sync with SHAPES_SPEC in
# generate_synthetic_csv.py so this script stays usable without importing it.
SHAPE_SEGMENTS: dict[str, list[tuple[str, float]]] = {
    "triangle":    [("+", 0.50), ("-", 0.50)],
    "v_shape":     [("-", 0.50), ("+", 0.50)],
    "double_peak": [("+", 0.25), ("-", 0.25), ("+", 0.25), ("-", 0.25)],
    "sawtooth":    [("+", 0.125), ("-", 0.125), ("+", 0.125), ("-", 0.125),
                    ("+", 0.125), ("-", 0.125), ("+", 0.125), ("-", 0.125)],
}
MIN_BUCKETS_PER_SEGMENT = 5
MIN_SEGMENTS_PER_PATTERN = 2


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("dataset",
                   help="dataset name; inputs at ${DATA_DIR}/<dataset>.csv and "
                        "${DATA_DIR}/<dataset>.csv.patterns.csv "
                        "(DATA_DIR defaults to /opt/exp-data); output at "
                        "queries/<dataset>/queries.txt")

    seq = p.add_argument_group("sequence shape")
    seq.add_argument("--seq-count", type=int, default=50,
                     help="number of queries to emit (default: 50)")
    seq.add_argument("--initial-fraction", type=float, default=0.1,
                     help="what fraction of the dataset Q0 covers (default: 0.1)")
    seq.add_argument("--measure", default="0", help="measure id (default: 0)")
    seq.add_argument("--width", type=int, default=1000)
    seq.add_argument("--height", type=int, default=600)
    seq.add_argument("--accuracy", type=float, default=0.95)
    seq.add_argument("--seed", type=int, default=42)

    walk = p.add_argument_group("Markov-walk operation probabilities (renormalised)")
    walk.add_argument("--pan-prob",      type=float, default=0.25)
    walk.add_argument("--zoom-in-prob",  type=float, default=0.10)
    walk.add_argument("--zoom-out-prob", type=float, default=0.10)
    walk.add_argument("--resize-prob",   type=float, default=0.05)
    walk.add_argument("--pattern-prob",  type=float, default=0.50)

    return p.parse_args()


# ----------------------------------------------------------------------------
# Time-range resolution
# ----------------------------------------------------------------------------


def parse_csv_time(s: str) -> int:
    """Parse a timestamp string into epoch ms, treating naive datetimes as UTC.
    Supports either ``YYYY-MM-DD HH:MM:SS[.fff]`` or ``YYYY-MM-DD``."""
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            tdt = dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc)
            return int(tdt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"cannot parse timestamp: {s!r}")


def peek_csv_range(path: str) -> Tuple[int, int]:
    """Return (first_ts_ms, last_ts_ms) by reading the first and last data lines.
    Robust to any header; assumes timestamp is column 0."""
    with open(path, "rb") as fh:
        # First data line = line 2 (after header).
        fh.seek(0)
        header = fh.readline()
        first = fh.readline().decode("utf-8", errors="replace")
        # Last data line: seek to near end.
        size = fh.seek(0, os.SEEK_END)
        block = min(64 * 1024, size)
        fh.seek(size - block)
        tail = fh.read().decode("utf-8", errors="replace")
        last = next(filter(None, reversed([ln.strip() for ln in tail.splitlines()])))
    first_ts = parse_csv_time(first.split(",")[0])
    last_ts = parse_csv_time(last.split(",")[0])
    return first_ts, last_ts


def resolve_range(csv_path: str) -> Tuple[int, int]:
    return peek_csv_range(csv_path)


# ----------------------------------------------------------------------------
# Output formatting
# ----------------------------------------------------------------------------


def fmt_ts(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f")[:-3]


def visual_line(f: int, t: int, args: argparse.Namespace, label: str) -> str:
    return ",".join([
        str(f), str(t), fmt_ts(f), fmt_ts(t),
        str(args.measure), str(args.width), str(args.height), f"{args.accuracy}",
        "visual", label,
    ])


def pattern_line(f: int, t: int, args: argparse.Namespace,
                 time_unit: str, spec: str) -> str:
    return ",".join([
        str(f), str(t), fmt_ts(f), fmt_ts(t),
        str(args.measure), str(args.width), str(args.height), f"{args.accuracy}",
        "pattern", "custom", time_unit, spec,
    ])


# ----------------------------------------------------------------------------
# Pattern-query construction
# ----------------------------------------------------------------------------


def _segments_for(shape: str, segments_field: str) -> Optional[list[tuple[str, float]]]:
    """Parse the ground-truth ``segments`` column (``sign:frac;sign:frac…``) and
    fall back to the static shape registry when older GT files lack the column.
    Returns ``None`` for unknown shapes so callers can skip them."""
    if segments_field:
        out: list[tuple[str, float]] = []
        for tok in segments_field.split(";"):
            tok = tok.strip()
            if not tok:
                continue
            sign, _, frac = tok.partition(":")
            if sign not in SIGN_TO_DEG:
                return None
            out.append((sign, float(frac)))
        if out:
            return out
    return SHAPE_SEGMENTS.get(shape)


def _allocate_buckets(segments: list[tuple[str, float]], total_buckets: int) -> Optional[list[int]]:
    """Split ``total_buckets`` across ``segments`` proportional to their fractions,
    enforcing ≥ MIN_BUCKETS_PER_SEGMENT per segment. Returns ``None`` if the total
    is too small to satisfy the floor across every segment."""
    n_seg = len(segments)
    if total_buckets < n_seg * MIN_BUCKETS_PER_SEGMENT:
        return None
    raw = [s[1] * total_buckets for s in segments]
    counts = [max(MIN_BUCKETS_PER_SEGMENT, int(round(r))) for r in raw]
    diff = total_buckets - sum(counts)
    # Fix rounding drift by adjusting the longest segment.
    if diff != 0:
        idx = max(range(n_seg), key=lambda i: counts[i])
        counts[idx] = max(MIN_BUCKETS_PER_SEGMENT, counts[idx] + diff)
    if sum(counts) <= 0 or any(c < MIN_BUCKETS_PER_SEGMENT for c in counts):
        return None
    return counts


def shape_to_spec(shape: str, segments_field: str = "",
                  pattern_ms: Optional[int] = None,
                  unit_ms: Optional[int] = None) -> Optional[str]:
    """Build a ``len:minDeg:maxDeg[;…]`` spec for a GT pattern.

    Falls back to a single-segment ``1:minDeg:maxDeg`` form when called without
    ``pattern_ms``/``unit_ms`` (legacy callers, random pattern path)."""
    segments = _segments_for(shape, segments_field)
    if not segments:
        return None
    if pattern_ms is None or unit_ms is None:
        # Legacy single-segment spec; use the first segment's slope sign.
        lo, hi = SIGN_TO_DEG[segments[0][0]]
        return f"1:{lo}:{hi}"
    total = max(1, pattern_ms // unit_ms)
    counts = _allocate_buckets(segments, total)
    if counts is None:
        return None
    return ";".join(
        f"{n}:{SIGN_TO_DEG[sign][0]}:{SIGN_TO_DEG[sign][1]}"
        for (sign, _frac), n in zip(segments, counts)
    )


def pick_time_unit_for_pattern(window_ms: int, pattern_ms: int,
                               n_segments: int = 1,
                               rng: Optional[random.Random] = None,
                               ) -> Optional[str]:
    """Pick a viewport-natural time unit (window/TARGET_VIEWPORT_BUCKETS) and
    step ``[STEPS_BELOW_MIN, STEPS_BELOW_MAX]`` positions finer in the
    TIME_UNITS_MS list. Constraints unchanged: every segment must fit
    ≥ MIN_BUCKETS_PER_SEGMENT and the viewport ≤ MAX_BUCKETS_IN_WINDOW.

    Why: uniform random over all valid units made pattern queries jump from
    15 min to 12 hours on identical viewports, which doesn't match how a user
    actually pans/zooms. Anchoring to the viewport-natural unit (and
    perturbing only a few finer steps) keeps the bucket size correlated with
    zoom level."""
    min_total_buckets = max(MIN_BUCKETS_IN_WINDOW, n_segments * MIN_BUCKETS_PER_SEGMENT)
    valid: List[Tuple[int, str]] = []
    for unit_ms, label in TIME_UNITS_MS:
        if pattern_ms < min_total_buckets * unit_ms:
            continue
        if window_ms / unit_ms > MAX_BUCKETS_IN_WINDOW:
            continue
        valid.append((unit_ms, label))
    if not valid:
        return None
    if rng is None:
        return valid[0][1]

    target_unit_ms = window_ms / TARGET_VIEWPORT_BUCKETS
    # Closest valid unit to target — i.e. the one TARGET_VIEWPORT_BUCKETS-ish
    # bucket-count would prefer. Then step 2 to 8 finer (clamped to index 0).
    natural_idx = min(range(len(valid)),
                      key=lambda i: abs(valid[i][0] - target_unit_ms))
    steps = rng.randint(STEPS_BELOW_MIN, STEPS_BELOW_MAX)
    chosen_idx = max(0, natural_idx - steps)
    return valid[chosen_idx][1]


def build_pattern_query_anchored(
    f: int, t: int, args: argparse.Namespace,
    gt: List[Tuple[int, int, int, str, str]], rng: random.Random,
) -> Optional[str]:
    window = t - f
    min_dur = int(window * MIN_PATTERN_FRACTION)
    max_dur = int(window * MAX_PATTERN_FRACTION)
    candidates = [
        p for p in gt
        if f <= p[0] and p[1] <= t and min_dur <= p[2] <= max_dur
    ]
    if not candidates:
        return None
    rng.shuffle(candidates)
    for cand in candidates:
        shape = cand[3]
        seg_field = cand[4]
        n_seg = len(_segments_for(shape, seg_field) or [])
        if n_seg < MIN_SEGMENTS_PER_PATTERN:
            continue
        tu_label = pick_time_unit_for_pattern(window, cand[2], n_seg, rng)
        if tu_label is None:
            continue
        unit_ms = next(u for u, lab in TIME_UNITS_MS if lab == tu_label)
        spec = shape_to_spec(shape, seg_field, cand[2], unit_ms)
        if spec is None:
            continue
        return pattern_line(f, t, args, tu_label, spec)
    return None


# ----------------------------------------------------------------------------
# Markov walk over visual operations
# ----------------------------------------------------------------------------


def walk_step(
    cur_from: int, cur_to: int, ds_from: int, ds_to: int,
    op: str, rng: random.Random,
) -> Tuple[int, int]:
    """Apply `op` to the current viewport, clamped to the dataset bounds.
    Returns the new (from, to)."""
    width = cur_to - cur_from
    if op == "pan":
        shift = int(rng.uniform(0.1, 0.5) * width) * rng.choice([-1, 1])
        new_from = max(ds_from, min(ds_to - width, cur_from + shift))
        return new_from, new_from + width
    if op == "zoomIn":
        new_width = max(width // 2, 1000)  # ≥ 1 second
        center = (cur_from + cur_to) // 2
        new_from = max(ds_from, center - new_width // 2)
        new_to = min(ds_to, new_from + new_width)
        return new_from, new_to
    if op == "zoomOut":
        new_width = min(width * 2, ds_to - ds_from)
        center = (cur_from + cur_to) // 2
        new_from = max(ds_from, center - new_width // 2)
        new_to = min(ds_to, new_from + new_width)
        return new_from, new_to
    if op == "resize":
        # Resize is a viewport (pixel) change, not a time-range change. Keep the
        # range as-is; the harness records it as a fresh query.
        return cur_from, cur_to
    raise ValueError(f"unknown op: {op}")


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    data_dir = os.environ.get("DATA_DIR", "/opt/exp-data")
    csv_path = f"{data_dir}/{args.dataset}.csv"
    patterns_path = f"{data_dir}/{args.dataset}.csv.patterns.csv"
    out_path = f"queries/{args.dataset}/queries.txt"

    if not os.path.isfile(csv_path):
        raise SystemExit(f"missing dataset CSV: {csv_path}")
    if not os.path.isfile(patterns_path):
        raise SystemExit(f"missing patterns sidecar: {patterns_path} "
                         f"(run scripts/data/detect_patterns.py on the real CSV "
                         f"or scripts/data/generate_synthetic_csv.py for synthetic)")

    ds_from, ds_to = resolve_range(csv_path)
    if ds_to <= ds_from:
        raise SystemExit(f"empty dataset range: {ds_from} >= {ds_to}")

    # Ground-truth patterns. The ``segments`` column carries the multi-segment
    # slope-sign program for each pattern; older sidecars without it fall back
    # to the static SHAPE_SEGMENTS map for known shape names.
    gt: List[Tuple[int, int, int, str, str]] = []
    with open(patterns_path) as fh:
        for r in csv.DictReader(fh):
            f_ms = int(r["from_ms"])
            t_ms = int(r["to_ms"])
            gt.append((f_ms, t_ms, t_ms - f_ms, r["shape"], r.get("segments", "")))
    gt.sort(key=lambda p: p[0])
    shape_hist: dict[str, int] = {}
    for p in gt:
        shape_hist[p[3]] = shape_hist.get(p[3], 0) + 1
    print(f"loaded {len(gt)} ground-truth patterns from {patterns_path} "
          f"(shapes: {shape_hist})", file=sys.stderr)
    if not gt:
        raise SystemExit(f"patterns sidecar {patterns_path} is empty")

    # Renormalise visual-op probabilities (excluding pattern).
    visual_ops = ["pan", "zoomIn", "zoomOut", "resize"]
    visual_weights = [args.pan_prob, args.zoom_in_prob,
                      args.zoom_out_prob, args.resize_prob]
    sw = sum(visual_weights)
    if sw <= 0:
        raise SystemExit("at least one of --pan-prob/--zoom-in/--zoom-out/--resize must be > 0")
    visual_weights = [w / sw for w in visual_weights]

    # Q0 — initial viewport covering --initial-fraction. Anchor on the densest
    # init_w window of GT patterns so subsequent pattern queries have anchors
    # to land on instead of getting stranded in a pattern-free region.
    full = ds_to - ds_from
    init_w = int(full * args.initial_fraction)
    init_w = max(1000, min(init_w, full))
    gt_starts = [p[0] for p in gt]
    i = 0
    best_count, best_start = 0, gt_starts[0]
    for j in range(len(gt_starts)):
        while gt_starts[j] - gt_starts[i] > init_w:
            i += 1
        if j - i + 1 > best_count:
            best_count = j - i + 1
            best_start = gt_starts[i]
    cur_from = max(ds_from, best_start - init_w // 4)
    cur_to = min(ds_to, cur_from + init_w)
    cur_from = cur_to - init_w
    print(f"Q0 anchored on densest {init_w} ms window ({best_count} GT patterns): "
          f"{cur_from}..{cur_to}", file=sys.stderr)

    out_lines: List[str] = [visual_line(cur_from, cur_to, args, "initial")]
    n_visual = 1
    n_pattern = 0

    while len(out_lines) < args.seq_count:
        # Decide pattern vs visual.
        if rng.random() < args.pattern_prob:
            line = build_pattern_query_anchored(cur_from, cur_to, args, gt, rng)
            if line is not None:
                out_lines.append(line)
                n_pattern += 1
                continue
            # No GT pattern fits this viewport — fall through to a visual op.

        op = rng.choices(visual_ops, weights=visual_weights, k=1)[0]
        cur_from, cur_to = walk_step(cur_from, cur_to, ds_from, ds_to, op, rng)
        out_lines.append(visual_line(cur_from, cur_to, args, op))
        n_visual += 1

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as fh:
        fh.write("\n".join(out_lines) + "\n")

    print(f"wrote {len(out_lines)} queries -> {out_path}  "
          f"(visual={n_visual}, pattern={n_pattern})",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
