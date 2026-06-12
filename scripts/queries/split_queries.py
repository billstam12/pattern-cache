#!/usr/bin/env python3
"""Split an interleaved queries file into vis-only and pattern-only subsets.

The siloed-composite experiment (paper Fig. 1a) requires running view queries
through one process and pattern queries through another, with no shared cache
state. This splitter prepares both subsets in one pass and writes a sidecar
mapping so analysis can reconstruct the original interleaved index order when
merging the two timing CSVs back together.

Input line format (column 9 is the type):

    from_ms,to_ms,from_str,to_str,measure,width,height,accuracy,type,label[,...]

Outputs (next to the input):

    <base>_vis.txt    — type=visual  lines, in original order
    <base>_pat.txt    — type=pattern lines, in original order
    <base>_split.csv  — orig_idx,subset,subset_idx  (header included)

Usage:

    python scripts/queries/split_queries.py queries/synth_10y_1m/queries.txt
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


TYPE_COL = 8  # 0-indexed; "type" is the 9th comma-separated column


def split(input_path: Path) -> tuple[Path, Path, Path]:
    base = input_path.with_suffix("")  # strip ".txt"
    vis_path = base.parent / f"{base.name}_vis.txt"
    pat_path = base.parent / f"{base.name}_pat.txt"
    map_path = base.parent / f"{base.name}_split.csv"

    vis_lines: list[str] = []
    pat_lines: list[str] = []
    mapping: list[tuple[int, str, int]] = []

    with input_path.open() as f:
        for orig_idx, line in enumerate(f):
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            cols = stripped.split(",")
            if len(cols) <= TYPE_COL:
                print(f"  warning: line {orig_idx} has < {TYPE_COL+1} cols, skipped",
                      file=sys.stderr)
                continue
            qtype = cols[TYPE_COL].strip().lower()
            if qtype == "visual":
                mapping.append((orig_idx, "vis", len(vis_lines)))
                vis_lines.append(stripped)
            elif qtype == "pattern":
                mapping.append((orig_idx, "pat", len(pat_lines)))
                pat_lines.append(stripped)
            else:
                print(f"  warning: line {orig_idx} unknown type {qtype!r}, skipped",
                      file=sys.stderr)

    vis_path.write_text("\n".join(vis_lines) + ("\n" if vis_lines else ""))
    pat_path.write_text("\n".join(pat_lines) + ("\n" if pat_lines else ""))
    with map_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["orig_idx", "subset", "subset_idx"])
        w.writerows(mapping)

    print(f"  {len(vis_lines)} vis → {vis_path}")
    print(f"  {len(pat_lines)} pat → {pat_path}")
    print(f"  {len(mapping)} mapping rows → {map_path}")
    return vis_path, pat_path, map_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", help="Path to the interleaved queries .txt file")
    args = ap.parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        sys.exit(f"input file not found: {input_path}")
    split(input_path)


if __name__ == "__main__":
    main()
