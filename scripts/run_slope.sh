#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Print each command before executing (helpful for debugging)
set -x

# sh ./scripts/run_method.sh --method m4 --patternMethod ols --mode timeM4Queries --table manufacturing_exp --queries "queries/guided/manufacturing_exp_queries_1p.txt" --out "output_slope" --runs 1
# sh ./scripts/run_method.sh --method m4 --patternMethod ols --mode timeM4Queries --table soccer_exp --queries "queries/guided/soccer_exp_queries_1p.txt" --out "output_slope" --runs 1
# sh ./scripts/run_method.sh --method m4 --patternMethod ols --mode timeM4Queries --table intel_lab_exp --queries "queries/guided/intel_lab_exp_queries_1p.txt" --out "output_slope" --runs 1

# sh ./scripts/run_method.sh --method minmax --mode timeCacheQueries --table manufacturing_exp --queries "queries/guided/manufacturing_exp_queries_1p.txt" --out "output_slope" --runs 1
# sh ./scripts/run_method.sh --method minmax --mode timeCacheQueries --table soccer_exp --queries "queries/guided/soccer_exp_queries_1p.txt" --out "output_slope" --runs 1
# sh ./scripts/run_method.sh --method minmax --mode timeCacheQueries --table intel_lab_exp --queries "queries/guided/intel_lab_exp_queries_1p.txt" --out "output_slope" --runs 1
