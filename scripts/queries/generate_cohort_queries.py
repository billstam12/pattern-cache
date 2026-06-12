#!/usr/bin/env python3
"""Emit per-axis cohort query files for the VASTA characteristics sweep.

Three independent axes vary one structural property of the pattern query while
the other two are held at a fixed "medium" setting. Each axis produces one
file per bucket under ``queries/<dataset>/cohorts/``.

Axes
----
  length       — #segments in the pattern: 2seg / 4seg / 8seg (BFS depth)
  width        — angular band radius around each segment's canonical slope:
                 narrow (±5°) / medium (±15°) / wide (±30°)
  selectivity  — expected #matches per query, driven by GT pattern duration:
                 few (long patterns, ≥ p75 dur) / med / many (≤ p25 dur)

Outputs
-------
  queries/<dataset>/cohorts/length_2seg.txt
  queries/<dataset>/cohorts/length_4seg.txt
  queries/<dataset>/cohorts/length_8seg.txt
  queries/<dataset>/cohorts/width_narrow.txt
  queries/<dataset>/cohorts/width_medium.txt
  queries/<dataset>/cohorts/width_wide.txt
  queries/<dataset>/cohorts/selectivity_few.txt
  queries/<dataset>/cohorts/selectivity_med.txt
  queries/<dataset>/cohorts/selectivity_many.txt

Each file holds ``--n-per-cohort`` (default 30) pattern queries in the same
line format as ``scripts/queries/generate_queries.py``.

Run
---

    python3 scripts/queries/generate_cohort_queries.py \\
        --csv /opt/exp-data/synth_10y_1m.csv \\
        --patterns /opt/exp-data/synth_10y_1m.csv.patterns.csv \\
        --dataset synth_10y_1m

The cohort experiment runner (``scripts/experiments.sh cohorts``) reads from
``queries/<dataset>/cohorts/`` directly; no other consumer needs to know the
cohort layout.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from pathlib import Path
from typing import List, Optional, Tuple

# Reuse the main generator's helpers — keeps the line format identical and
# avoids re-implementing window sizing.
from generate_queries import (
    MAX_BUCKETS_IN_WINDOW,
    MAX_PATTERN_FRACTION,
    MIN_BUCKETS_PER_SEGMENT,
    MIN_PATTERN_FRACTION,
    SHAPE_SEGMENTS,
    SIGN_TO_DEG,
    TIME_UNITS_MS,
    fmt_ts,
    peek_csv_range,
)


# Per-bucket angle-band radius (degrees), applied around each sign's canonical
# centre (rising:+60°, falling:-60°, flat:0°). The "wide" bucket matches the
# main generator's default (full quadrant); narrow squeezes the band to a
# 10°-wide window.
WIDTH_BUCKETS: dict[str, dict[str, Tuple[int, int]]] = {
    "narrow": {"+": (55, 65),  "-": (-65, -55),  "0": (-5, 5)},
    "medium": {"+": (45, 75),  "-": (-75, -45),  "0": (-10, 10)},
    "wide":   {"+": (30, 89),  "-": (-89, -30),  "0": (-10, 10)},
}

# Length buckets → shapes (must exist in SHAPE_SEGMENTS). Aligned with the
# 4-shape vocabulary: triangle/v_shape (2 seg), double_peak (4 seg), sawtooth
# (8 seg). Single- and 3-segment shapes were dropped from the registry.
LENGTH_BUCKETS: dict[str, list[str]] = {
    "2seg": ["triangle", "v_shape"],
    "4seg": ["double_peak"],
    "8seg": ["sawtooth"],
}

# Cohort axes use the "medium" setting for the two non-varying axes.
DEFAULT_LENGTH_BUCKET = "4seg"
DEFAULT_WIDTH_BUCKET = "medium"

# Per-cohort query count; same across cohorts for fair comparison.
N_PER_COHORT_DEFAULT = 30


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--csv", help="dataset CSV (used to peek time range when "
                                  "--from/--to omitted)")
    p.add_argument("--patterns", required=True,
                   help="GT patterns CSV (from_ms,to_ms,shape,...)")
    p.add_argument("--dataset", required=True,
                   help="dataset name; output goes to queries/<dataset>/cohorts/")
    p.add_argument("--from", dest="from_ms", type=int,
                   help="dataset start (epoch ms); else peeked from --csv")
    p.add_argument("--to", dest="to_ms", type=int,
                   help="dataset end (epoch ms); else peeked from --csv")
    p.add_argument("--n-per-cohort", type=int, default=N_PER_COHORT_DEFAULT)
    p.add_argument("--allow-repeats", action="store_true",
                   help="when the anchor pool is smaller than --n-per-cohort, "
                        "sample anchors with replacement so each cohort reaches "
                        "the target size. Each repeat draws a fresh random "
                        "viewport, so the queries differ even though they "
                        "share a GT region. Off by default — preserves the "
                        "one-query-per-anchor independence assumption.")
    p.add_argument("--measure", default="0")
    p.add_argument("--width", type=int, default=1000)
    p.add_argument("--height", type=int, default=600)
    p.add_argument("--accuracy", type=float, default=0.95)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def _resolve_range(args: argparse.Namespace) -> Tuple[int, int]:
    if args.from_ms is not None and args.to_ms is not None:
        return args.from_ms, args.to_ms
    if args.csv:
        return peek_csv_range(args.csv)
    raise SystemExit("specify dataset range via --from/--to or --csv")


def _load_patterns(path: str) -> list[dict]:
    out = []
    with open(path) as fh:
        for r in csv.DictReader(fh):
            out.append({
                "from_ms": int(r["from_ms"]),
                "to_ms":   int(r["to_ms"]),
                "shape":   r["shape"],
                "segments": r.get("segments", ""),
            })
    return out


def _segments_for(shape: str) -> list[tuple[str, float]]:
    """Use the static shape registry — ignores per-row segment text since cohorts
    select by canonical shape, not by individual instance."""
    return SHAPE_SEGMENTS.get(shape, [])


def _pick_time_unit(window_ms: int, pattern_ms: int, n_segments: int) -> Optional[str]:
    """Smallest time unit that gives ≥ MIN_BUCKETS_PER_SEGMENT per segment AND
    keeps the viewport ≤ MAX_BUCKETS_IN_WINDOW. Same policy as generate_queries.py."""
    min_total = max(5, n_segments * MIN_BUCKETS_PER_SEGMENT)
    for unit_ms, label in TIME_UNITS_MS:
        if pattern_ms < min_total * unit_ms:
            continue
        if window_ms / unit_ms > MAX_BUCKETS_IN_WINDOW:
            continue
        return label
    return None


def _allocate(segments: list[tuple[str, float]], total: int) -> Optional[list[int]]:
    n = len(segments)
    if total < n * MIN_BUCKETS_PER_SEGMENT:
        return None
    raw = [s[1] * total for s in segments]
    counts = [max(MIN_BUCKETS_PER_SEGMENT, int(round(r))) for r in raw]
    diff = total - sum(counts)
    if diff != 0:
        idx = max(range(n), key=lambda i: counts[i])
        counts[idx] = max(MIN_BUCKETS_PER_SEGMENT, counts[idx] + diff)
    if any(c < MIN_BUCKETS_PER_SEGMENT for c in counts):
        return None
    return counts


def _build_spec(shape: str, pattern_ms: int, unit_ms: int,
                width_bands: dict[str, Tuple[int, int]]) -> Optional[str]:
    """Build a ``count:lo:hi[;...]`` pattern spec using the given band table."""
    segments = _segments_for(shape)
    if not segments:
        return None
    total = max(1, pattern_ms // unit_ms)
    counts = _allocate(segments, total)
    if counts is None:
        return None
    parts = []
    for (sign, _frac), n in zip(segments, counts):
        lo, hi = width_bands.get(sign, SIGN_TO_DEG[sign])
        parts.append(f"{n}:{lo}:{hi}")
    return ";".join(parts)


def _viewport_for(pat: dict, ds_from: int, ds_to: int,
                  rng: random.Random) -> Optional[Tuple[int, int]]:
    """Random viewport containing ``pat`` such that pattern occupies between
    MIN_PATTERN_FRACTION and MAX_PATTERN_FRACTION of the viewport span."""
    pat_dur = pat["to_ms"] - pat["from_ms"]
    if pat_dur <= 0:
        return None
    # Window size: log-uniform between [pat_dur/max_frac, pat_dur/min_frac].
    lo_w = int(pat_dur / MAX_PATTERN_FRACTION)
    hi_w = int(pat_dur / MIN_PATTERN_FRACTION)
    if hi_w < lo_w:
        return None
    win = rng.randint(lo_w, hi_w)
    win = min(win, ds_to - ds_from)
    if win < pat_dur:
        return None
    slack = win - pat_dur
    pre = rng.randint(0, slack)
    f = pat["from_ms"] - pre
    t = f + win
    f = max(ds_from, f)
    t = min(ds_to, t)
    if t - f < pat_dur:
        return None
    return f, t


def _pattern_line(f: int, t: int, args: argparse.Namespace,
                  time_unit: str, spec: str) -> str:
    return ",".join([
        str(f), str(t), fmt_ts(f), fmt_ts(t),
        str(args.measure), str(args.width), str(args.height),
        f"{args.accuracy}", "pattern", "custom", time_unit, spec,
    ])


def _emit_cohort(
    out_path: Path,
    pool: list[dict],
    *,
    width_bands: dict[str, Tuple[int, int]],
    args: argparse.Namespace,
    ds_from: int,
    ds_to: int,
    rng: random.Random,
) -> int:
    """Draw queries from ``pool`` until ``args.n_per_cohort`` valid lines are
    emitted (or the pool is exhausted, when ``--allow-repeats`` is off).
    Returns the number written.

    With ``--allow-repeats``: sample anchors uniformly with replacement so
    a small pool can still fill the cohort; each draw gets a fresh random
    viewport via ``_viewport_for``, so the emitted queries differ in time
    unit, viewport span, and pattern position even when the underlying
    anchor repeats. Cap iterations at ``8 * n_per_cohort`` so a pool of
    only-bad anchors (all viewport draws rejected) doesn't loop forever."""
    pool = list(pool)
    rng.shuffle(pool)
    lines: list[str] = []

    def _try_emit(pat: dict) -> Optional[str]:
        viewport = _viewport_for(pat, ds_from, ds_to, rng)
        if viewport is None:
            return None
        f, t = viewport
        pat_ms = pat["to_ms"] - pat["from_ms"]
        n_seg = len(_segments_for(pat["shape"]))
        tu_label = _pick_time_unit(t - f, pat_ms, n_seg)
        if tu_label is None:
            return None
        unit_ms = next(u for u, lab in TIME_UNITS_MS if lab == tu_label)
        spec = _build_spec(pat["shape"], pat_ms, unit_ms, width_bands)
        if spec is None:
            return None
        return _pattern_line(f, t, args, tu_label, spec)

    if getattr(args, "allow_repeats", False) and pool:
        max_iters = 8 * args.n_per_cohort
        for _ in range(max_iters):
            if len(lines) >= args.n_per_cohort:
                break
            pat = rng.choice(pool)
            line = _try_emit(pat)
            if line is not None:
                lines.append(line)
    else:
        for pat in pool:
            if len(lines) >= args.n_per_cohort:
                break
            line = _try_emit(pat)
            if line is not None:
                lines.append(line)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + ("\n" if lines else ""))
    return len(lines)


def main() -> int:
    args = parse_args()
    rng_root = random.Random(args.seed)
    ds_from, ds_to = _resolve_range(args)
    if ds_to <= ds_from:
        raise SystemExit(f"empty dataset range: {ds_from}>={ds_to}")

    patterns = _load_patterns(args.patterns)
    if not patterns:
        raise SystemExit(f"no patterns loaded from {args.patterns}")

    out_root = Path("queries") / args.dataset / "cohorts"

    # ── Length axis: filter by shape group, fix width = medium.
    width_med = WIDTH_BUCKETS[DEFAULT_WIDTH_BUCKET]
    for bucket, shapes in LENGTH_BUCKETS.items():
        pool = [p for p in patterns if p["shape"] in shapes]
        n = _emit_cohort(out_root / f"length_{bucket}.txt", pool,
                         width_bands=width_med, args=args,
                         ds_from=ds_from, ds_to=ds_to,
                         rng=random.Random(rng_root.random()))
        print(f"  length_{bucket}: {n} queries ({len(pool)} candidates from {shapes})")

    # ── Width axis: fix length = 2seg, vary band radius.
    pool_2seg = [p for p in patterns if p["shape"] in LENGTH_BUCKETS[DEFAULT_LENGTH_BUCKET]]
    for bucket, bands in WIDTH_BUCKETS.items():
        n = _emit_cohort(out_root / f"width_{bucket}.txt", pool_2seg,
                         width_bands=bands, args=args,
                         ds_from=ds_from, ds_to=ds_to,
                         rng=random.Random(rng_root.random()))
        print(f"  width_{bucket}: {n} queries (bands={bands})")

    # ── Selectivity axis: fix length = 2seg & width = medium; bucket by GT
    # pattern duration. Short patterns admit many candidate windows in the
    # synthetic data (low selectivity / many matches); long patterns the
    # opposite. Quartile split on duration over the 2-seg shape pool.
    durations = sorted(p["to_ms"] - p["from_ms"] for p in pool_2seg)
    if len(durations) < 4:
        print("  selectivity: not enough 2-seg patterns to bucket by duration")
    else:
        q25 = durations[len(durations) // 4]
        q75 = durations[(3 * len(durations)) // 4]
        many_pool = [p for p in pool_2seg if (p["to_ms"] - p["from_ms"]) <= q25]
        med_pool  = [p for p in pool_2seg if q25 < (p["to_ms"] - p["from_ms"]) <= q75]
        few_pool  = [p for p in pool_2seg if (p["to_ms"] - p["from_ms"]) >  q75]
        for bucket, pool in (("many", many_pool), ("med", med_pool), ("few", few_pool)):
            n = _emit_cohort(out_root / f"selectivity_{bucket}.txt", pool,
                             width_bands=width_med, args=args,
                             ds_from=ds_from, ds_to=ds_to,
                             rng=random.Random(rng_root.random()))
            print(f"  selectivity_{bucket}: {n} queries "
                  f"(pool={len(pool)}, dur quartile cut at q25={q25} q75={q75})")

    print(f"wrote cohort files under {out_root}/", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
