#!/usr/bin/env python3
"""Stream a wide-format time-series CSV into the long (timestamp, id, value)
schema the Postgres/Timescale loader expects (DataSourceFactory.java:213).

Wide format (multiple value columns per row):

    datetime,1,2,3,4,5,6,7
    2012-02-22 21:42:11.000,4322325,14436,15737,8484,84,190,148
    ...

Long format (one row per (timestamp, measure)):

    timestamp,id,value
    2012-02-22 21:42:11.000,2,14436
    ...

Streams line-by-line, no full-file load in memory — works on the 30GB
soccer dump.

Usage:
    python3 scripts/data/wide_to_long.py \\
        --in /opt/exp-data/manufacturing.csv --time-col datetime --measures 2 \\
        --out /opt/exp-data/manufacturing.csv.long

For multi-measure pivots pass --measures col1,col2,col3 — output emits one
row per (timestamp, measure) pair so the row count multiplies by the
number of measures.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--in", dest="in_path", required=True, help="input wide CSV")
    p.add_argument("--time-col", default="datetime",
                   help="timestamp column name (default: datetime)")
    p.add_argument("--measures", required=True,
                   help="comma-separated value column names to emit as long rows")
    p.add_argument("--out", required=True, help="output long-format CSV")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    measures = [m.strip() for m in args.measures.split(",") if m.strip()]
    if not measures:
        raise SystemExit("--measures must list at least one column")

    rows_in = 0
    rows_out = 0
    t0 = time.time()
    with open(args.in_path, "r", newline="") as fin, \
         open(args.out, "w", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        try:
            header = next(reader)
        except StopIteration:
            raise SystemExit(f"empty input: {args.in_path}")
        try:
            ts_idx = header.index(args.time_col)
        except ValueError:
            raise SystemExit(f"time column {args.time_col!r} not in header: {header}")
        meas_idx = []
        for m in measures:
            try:
                meas_idx.append((m, header.index(m)))
            except ValueError:
                raise SystemExit(f"measure column {m!r} not in header: {header}")
        writer.writerow(["timestamp", "id", "value"])
        for row in reader:
            rows_in += 1
            ts = row[ts_idx]
            for mname, mi in meas_idx:
                writer.writerow([ts, mname, row[mi]])
                rows_out += 1
            if rows_in % 1_000_000 == 0:
                dt_s = time.time() - t0
                print(f"  {rows_in:_} input rows ({rows_out:_} out) "
                      f"in {dt_s:.1f}s",
                      file=sys.stderr)
    print(f"done: {rows_in:_} input rows -> {rows_out:_} output rows "
          f"({args.out})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
