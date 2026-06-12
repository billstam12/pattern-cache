#!/usr/bin/env bash
# Generate every query file for one dataset in a single shot:
#
#   1. Interleaved Markov-walk workload (vis + pattern) → queries/<dataset>/queries.txt
#   2. Vis-only / pat-only split + index sidecar         → queries/<dataset>/queries_{vis,pat}.txt + queries_split.csv
#   3. ρ_P-sweep variants (rewrites col 8 = accuracy)    → queries/<dataset>/queries_acc{0.99,0.95,…}.txt
#   4. §5.6 cohort files (length / width / selectivity)  → queries/<dataset>/cohorts/*.txt
#
# Single required argument is the dataset name. The script expects
# ``${DATA_DIR}/<dataset>.csv`` (raw time series) and
# ``${DATA_DIR}/<dataset>.csv.patterns.csv`` (ground-truth ramps from
# ``scripts/data/generate_synthetic_csv.py``) to already exist.
# ``DATA_DIR`` defaults to ``/opt/exp-data``.
#
# Usage:
#   scripts/queries/generate_all.sh <dataset>
#
# Examples:
#   scripts/queries/generate_all.sh synth_10y_1m
#   scripts/queries/generate_all.sh synth_10y_5s

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <dataset>" >&2
  exit 1
fi

DATASET="$1"
DATA_DIR="${DATA_DIR:-/opt/exp-data}"
CSV="${DATA_DIR}/${DATASET}.csv"
PATTERNS="${DATA_DIR}/${DATASET}.csv.patterns.csv"
OUT_DIR="queries/${DATASET}"

if [[ ! -f "$CSV" ]]; then
  echo "$0: data file not found: $CSV" >&2
  echo "  generate via scripts/data/generate_synthetic_datasets.sh" >&2
  exit 2
fi
if [[ ! -f "$PATTERNS" ]]; then
  echo "$0: patterns file not found: $PATTERNS" >&2
  echo "  generate via scripts/data/generate_synthetic_csv.py (writes both CSV and .patterns.csv)" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$OUT_DIR"

# Default ρ_P sweep; override via ``ACCS="..."`` env to match what
# ``experiments.sh risk`` reads.
ACCS="${ACCS:-0.99 0.95 0.9 0.8 0.5}"

echo "=== 1/4: interleaved workload (vis + pattern) ==="
python3 "$SCRIPT_DIR/generate_queries.py" "$DATASET" \
  --seq-count 50 --initial-fraction 0.75

echo
echo "=== 2/4: vis-only / pat-only split ==="
python3 "$SCRIPT_DIR/split_queries.py" "$OUT_DIR/queries.txt"

echo
echo "=== 3/4: ρ_P-sweep variants (ACCS=\"$ACCS\") ==="
for A in $ACCS; do
  QFILE="$OUT_DIR/queries_acc${A}.txt"
  awk -v a="$A" 'BEGIN{FS=OFS=","} {$8=a; print}' \
    "$OUT_DIR/queries.txt" > "$QFILE"
  echo "  wrote $QFILE"
done

echo
echo "=== 4/4: cohort files (§5.6 characteristics sweep) ==="
# --allow-repeats so cohorts whose anchor pool is short (e.g. only a handful
# of double_peak instances on a real dataset) still hit n_per_cohort by
# resampling the same anchors under fresh random viewports. On synth where
# every shape has thousands of anchors this is a no-op — the loop hits
# n_per_cohort long before any anchor would repeat.
python3 "$SCRIPT_DIR/generate_cohort_queries.py" \
  --csv "$CSV" --patterns "$PATTERNS" --dataset "$DATASET" --allow-repeats

echo
echo "All query files for ${DATASET} written under ${OUT_DIR}/"
ls -1 "$OUT_DIR"
