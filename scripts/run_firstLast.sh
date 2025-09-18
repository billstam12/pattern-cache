#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Print each command before executing (helpful for debugging)
set -x

sh ./scripts/run_method.sh --type influx --method firstLast --mode timeAggregateQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method firstLast --mode timeAggregateQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method firstLast --mode timeAggregateQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_no_allocation" --runs 5

sh ./scripts/run_method.sh --type influx --method minMax --mode timeMinMaxCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_no_allocation"  --runs 5
sh ./scripts/run_method.sh --type influx --method minMax --mode timeMinMaxCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_no_allocation"  --runs 5
sh ./scripts/run_method.sh --type influx --method minMax --mode timeMinMaxCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_no_allocation"  --runs 5

sh ./scripts/run_method.sh --type influx --method m4Inf --mode timeCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method m4Inf --mode timeCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method m4Inf --mode timeCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_no_allocation" --runs 5

sh ./scripts/run_method.sh --type influx --method minMax --mode timeCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_no_allocation" --runs 5 
sh ./scripts/run_method.sh --type influx --method minMax --mode timeCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_no_allocation" --runs 5 
sh ./scripts/run_method.sh --type influx --method minMax --mode timeCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_no_allocation" --runs 5 

sh ./scripts/run_method.sh --type influx --method m4 --mode timeCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method m4 --mode timeCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_no_allocation" --runs 5
sh ./scripts/run_method.sh --type influx --method m4 --mode timeCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_no_allocation" --runs 5

#  ALLOCATION
sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.01 --measures 1 --method m4Inf --mode timeCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_with_allocation_1p" --runs 5
sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.01 --measures 9 --method m4Inf --mode timeCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_with_allocation_1p" --runs 5
sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.01 --measures 2 --method m4Inf --mode timeCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_with_allocation_1p" --runs 5

sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.1 --measures 9 --method m4Inf --mode timeCacheQueries --table soccer_exp --queries "queries/mid_acc/big_seg/soccer_exp_queries_100p.txt" --out "output_with_allocation_10p" --runs 5
sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.1 --measures 1 --method m4Inf --mode timeCacheQueries --table manufacturing_exp --queries "queries/mid_acc/big_seg/manufacturing_exp_queries_100p.txt" --out "output_with_allocation_10p" --runs 5
sh ./scripts/run_method.sh --type influx --initCacheAllocation 0.1 --measures 2 --method m4Inf --mode timeCacheQueries --table intel_lab_exp --queries "queries/mid_acc/big_seg/intel_lab_exp_queries_100p.txt" --out "output_with_allocation_10p" --runs 5
