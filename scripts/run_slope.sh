#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Print each command before executing (helpful for debugging)
set -x

sh ./scripts/run_method.sh  --type trino --method ols --mode timeAggregateQueries --table manufacturing_exp --queries "queries/guided/manufacturing_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method ols --mode timeAggregateQueries --table soccer_exp --queries "queries/guided/soccer_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method ols --mode timeAggregateQueries --table intel_lab_exp --queries "queries/guided/intel_lab_exp_queries_1p.txt" --out "output_slope" --runs 1

sh ./scripts/run_method.sh  --type trino --method approxOls --mode timeCacheQueries --table manufacturing_exp --queries "queries/guided/manufacturing_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method approxOls --mode timeCacheQueries --table soccer_exp --queries "queries/guided/soccer_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method approxOls --mode timeCacheQueries --table intel_lab_exp --queries "queries/guided/intel_lab_exp_queries_1p.txt" --out "output_slope" --runs 1

sh ./scripts/run_method.sh  --type trino --method ols --mode timeMatchRecognizeQueries --table manufacturing_exp --queries "queries/guided/manufacturing_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method ols --mode timeMatchRecognizeQueries --table soccer_exp --queries "queries/guided/soccer_exp_queries_1p.txt" --out "output_slope" --runs 1
sh ./scripts/run_method.sh  --type trino --method ols --mode timeMatchRecognizeQueries --table intel_lab_exp --queries "queries/guided/intel_lab_exp_queries_1p.txt" --out "output_slope" --runs 1
