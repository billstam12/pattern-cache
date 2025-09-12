#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Print each command before executing (helpful for debugging)
set -x


# Run all mid_acc combinations (all segment sizes)
for SEG in small mid big; do
	ACC_TYPE="mid"
	PATTERN_SIZE="$SEG"
	OUT_FOLDER="output_slope_${ACC_TYPE}_acc_${PATTERN_SIZE}_seg"
	QUERIES_FOLDER="queries/${ACC_TYPE}_acc/${PATTERN_SIZE}_seg"
	ACC_STR="1p"

	# timeAggregateQueries
	sh ./scripts/run_method.sh --type trino --method ols --mode timeAggregateQueries --table manufacturing_exp --queries "${QUERIES_FOLDER}/manufacturing_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1
	sh ./scripts/run_method.sh --type trino --method ols --mode timeAggregateQueries --table soccer_exp --queries "${QUERIES_FOLDER}/soccer_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1
	sh ./scripts/run_method.sh --type trino --method ols --mode timeAggregateQueries --table intel_lab_exp --queries "${QUERIES_FOLDER}/intel_lab_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1

	# timeMatchRecognizeQueries
	sh ./scripts/run_method.sh --type trino --method ols --mode timeMatchRecognizeQueries --table manufacturing_exp --queries "${QUERIES_FOLDER}/manufacturing_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1
	sh ./scripts/run_method.sh --type trino --method ols --mode timeMatchRecognizeQueries --table soccer_exp --queries "${QUERIES_FOLDER}/soccer_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1
	sh ./scripts/run_method.sh --type trino --method ols --mode timeMatchRecognizeQueries --table intel_lab_exp --queries "${QUERIES_FOLDER}/intel_lab_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1

	# timeCacheQueries with adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table manufacturing_exp --queries "${QUERIES_FOLDER}/manufacturing_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table soccer_exp --queries "${QUERIES_FOLDER}/soccer_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table intel_lab_exp --queries "${QUERIES_FOLDER}/intel_lab_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation

	# timeCacheQueries NO adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table manufacturing_exp --queries "${QUERIES_FOLDER}/manufacturing_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}_no_adapt" --runs 1
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table soccer_exp --queries "${QUERIES_FOLDER}/soccer_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}_no_adapt" --runs 1
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table intel_lab_exp --queries "${QUERIES_FOLDER}/intel_lab_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}_no_adapt" --runs 1
done

# Run low_acc and high_acc only for mid_seg
for ACC_TYPE in low high; do
	PATTERN_SIZE="mid"
	OUT_FOLDER="output_slope_${ACC_TYPE}_acc_${PATTERN_SIZE}_seg"
	QUERIES_FOLDER="queries/${ACC_TYPE}_acc/${PATTERN_SIZE}_seg"
	ACC_STR="1p"
	# timeCacheQueries with adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table manufacturing_exp --queries "${QUERIES_FOLDER}/manufacturing_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table soccer_exp --queries "${QUERIES_FOLDER}/soccer_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation
	sh ./scripts/run_method.sh --type trino --method approxOls --mode timeCacheQueries --table intel_lab_exp --queries "${QUERIES_FOLDER}/intel_lab_exp_queries_${ACC_STR}.txt" --out "${OUT_FOLDER}" --runs 1 --adaptation
done

