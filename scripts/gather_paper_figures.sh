#!/usr/bin/env bash
# Copy every paper figure listed in scripts/paper_figures.list into a single
# upload-ready folder. Source paths are repo-relative; the destination is
# paper_figures/<basename>. A '.png' sibling is copied too when present.
#
# Usage:
#   scripts/gather_paper_figures.sh [list_file [out_dir]]
#
# Defaults:
#   list_file = scripts/paper_figures.list
#   out_dir   = paper_figures
#
# Exit codes: 0 if every entry landed, 1 if any source is missing.
#
# Edit scripts/paper_figures.list to swap a figure between datasets/variants
# without touching the .tex — the .tex keeps referring to body/plots/<basename>.

set -euo pipefail

LIST="${1:-scripts/paper_figures.list}"
OUT="${2:-paper_figures}"

if [[ ! -f "$LIST" ]]; then
  echo "list file not found: $LIST" >&2
  exit 2
fi

mkdir -p "$OUT"

MISSING=()
COPIED=0
TOTAL=0
while IFS= read -r raw; do
  # Strip comment + trim whitespace; skip blanks.
  line="${raw%%#*}"
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [[ -z "$line" ]] && continue
  TOTAL=$((TOTAL + 1))
  if [[ ! -f "$line" ]]; then
    MISSING+=("$line")
    continue
  fi
  base=$(basename "$line")
  cp -f "$line" "$OUT/$base"
  # If a .png sibling exists, take it too (handy for previewing).
  png="${line%.pdf}.png"
  [[ -f "$png" ]] && cp -f "$png" "$OUT/$(basename "$png")"
  COPIED=$((COPIED + 1))
done < "$LIST"

echo "Copied $COPIED of $TOTAL figures into $OUT/"
if [[ ${#MISSING[@]} -gt 0 ]]; then
  echo
  echo "MISSING (${#MISSING[@]}):"
  printf '  %s\n' "${MISSING[@]}"
  exit 1
fi
