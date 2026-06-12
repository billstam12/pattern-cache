#!/usr/bin/env bash
# Generate a family of synthetic datasets that share a fixed time span but
# vary in sampling interval. Each dataset gets its own relative-scale pattern
# ladder (see scripts/data/generate_synthetic_csv.py) so every zoom level has
# detectable ramps regardless of size.
#
# Usage:
#   scripts/data/generate_synthetic_datasets.sh [-s SPAN] [-o OUT_DIR] [-m MEASURES]
#                                          [-S START] [-d] [INTERVAL ...]
#
#   -s SPAN       dataset span as <N>{y|d|h|m|s|ms}. Default: 10y
#   -o OUT_DIR    output directory. Default: ${DATA_DIR:-/opt/exp-data}
#   -m MEASURES   comma-separated measure names. Default: synth
#   -S START      start timestamp YYYY-MM-DD HH:MM:SS. Default: 2024-01-01 00:00:00
#   -d            dry-run: print what would be generated, do not invoke
#   INTERVAL...   one or more sampling intervals (same syntax as -s).
#                 Default: 1h 10m 1m 1s 250ms
#
# Examples:
#   # Default 10-year sweep across 5 sampling rates
#   scripts/data/generate_synthetic_datasets.sh
#
#   # Custom span and a tighter set of intervals
#   scripts/data/generate_synthetic_datasets.sh -s 1y 1m 5s 1s 500ms
#
#   # Dry-run to preview row counts and disk sizes
#   scripts/data/generate_synthetic_datasets.sh -d

set -euo pipefail

SPAN="10y"
OUT_DIR="${DATA_DIR:-/opt/exp-data}"
MEASURES="synth"
START="2024-01-01 00:00:00"
DRY=0

while getopts ":s:o:m:S:dh" opt; do
  case "$opt" in
    s) SPAN="$OPTARG" ;;
    o) OUT_DIR="$OPTARG" ;;
    m) MEASURES="$OPTARG" ;;
    S) START="$OPTARG" ;;
    d) DRY=1 ;;
    h) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//' ; exit 0 ;;
    \?) echo "unknown option: -$OPTARG" >&2 ; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

# Default intervals if none specified
if [[ $# -eq 0 ]]; then
  set -- 5s
fi

# Parse a duration string like "10y", "30m", "250ms" into milliseconds.
parse_ms() {
  local s="$1"
  if [[ "$s" =~ ^([0-9]+)(ms|s|m|h|d|y)$ ]]; then
    local n="${BASH_REMATCH[1]}"
    case "${BASH_REMATCH[2]}" in
      ms) echo "$n" ;;
      s)  echo "$((n * 1000))" ;;
      m)  echo "$((n * 60000))" ;;
      h)  echo "$((n * 3600000))" ;;
      d)  echo "$((n * 86400000))" ;;
      y)  echo "$((n * 31557600000))" ;;  # 365.25 days
    esac
  else
    echo "bad duration: $s" >&2 ; return 2
  fi
}

SPAN_MS=$(parse_ms "$SPAN")
echo "span=$SPAN ($SPAN_MS ms), out_dir=$OUT_DIR, measures=$MEASURES, start='$START'"
echo "intervals: $*"
echo

mkdir -p "$OUT_DIR"

# Resolve python: prefer project .venv if present.
PY=python3
if [[ -x .venv/bin/python ]]; then PY=.venv/bin/python; fi

for I in "$@"; do
  I_MS=$(parse_ms "$I")
  ROWS=$((SPAN_MS / I_MS))
  OUT="$OUT_DIR/synth_${SPAN}_${I}.csv"

  # Rough disk estimate: ~36 bytes per row × measures.
  N_MEAS=$(awk -F, '{print NF}' <<<"$MEASURES")
  EST_MB=$(( ROWS * N_MEAS * 36 / 1048576 ))

  printf '==> %-40s rows=%-15d ~%dMB\n' "$OUT" "$ROWS" "$EST_MB"

  if [[ $DRY -eq 1 ]]; then
    continue
  fi

  "$PY" scripts/data/generate_synthetic_csv.py \
    --rows "$ROWS" \
    --interval-ms "$I_MS" \
    --start "$START" \
    --measures "$MEASURES" \
    --out "$OUT"
done

echo
echo "done."
