#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Print each command before executing (helpful for debugging)
set -x

sh ./scripts/run_method.sh --method m4 --mode timeQueries --table intel_lab_exp --queries "queries/influx_intel_lab_exp_queries_guided.txt" --out "output_no_allocation" --runs 1
sh ./scripts/run_method.sh --method m4 --mode timeQueries --table soccer_exp --queries "queries/influx_soccer_exp_queries_guided.txt" --out "output_no_allocation" --runs 1

sh ./scripts/run_method.sh --method m4Inf --mode timeCacheQueries --table intel_lab_exp --queries "queries/influx_intel_lab_exp_queries_guided.txt" --out "output_no_allocation" --runs 1
sh ./scripts/run_method.sh --method m4Inf --mode timeCacheQueries --table soccer_exp --queries "queries/influx_soccer_exp_queries_guided.txt" --out "output_no_allocation" --runs 1

# sh ./scripts/run_method.sh --method minmax --mode timeMinMaxCacheQueries --table intel_lab_exp --queries "queries/influx_intel_lab_exp_queries_guided.txt" --out "output_no_allocation"  --runs 5
# sh ./scripts/run_method.sh --method minmax --mode timeMinMaxCacheQueries --table soccer_exp --queries "queries/influx_soccer_exp_queries_guided.txt" --out "output_no_allocation"  --runs 5

# sh ./scripts/run_method.sh --initCacheAllocation 0.1 --method minmax --mode timeMinMaxCacheQueries --table intel_lab_exp --queries "queries/influx_intel_lab_exp_queries_guided.txt" --out "output_with_allocation" 
# sh ./scripts/run_method.sh --initCacheAllocation 0.1 --method minmax --mode timeMinMaxCacheQueries --table soccer_exp --queries "queries/influx_soccer_exp_queries_guided.txt" --out "output_with_allocation"

# sh ./scripts/run_method.sh --initCacheAllocation 0.1 --method m4Inf --mode timeCacheQueries --table intel_lab_exp --queries "queries/influx_intel_lab_exp_queries_guided.txt" --out "output_with_allocation"
# sh ./scripts/run_method.sh --initCacheAllocation 0.1 --method m4Inf --mode timeCacheQueries --table soccer_exp --queries "queries/influx_soccer_exp_queries_guided.txt" --out "output_with_allocation"
