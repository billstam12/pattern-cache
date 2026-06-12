#!/bin/bash

# Script to run pattern matching experiments with different methods
# This script executes the Experiments Java class with appropriate parameters

set -e  # Exit on error

# Default values for script parameters
METHOD="m4"
TYPE="trino"
MODE="timeCacheQueries"
RUNS=1
OUT_FOLDER="output"
CACHE_ALLOCATION=0
MEASURES="1"
SCHEMA="more"
TABLE="intel_lab_exp"
QUERIES=""
CSV=""
REFINEMENT=""
AGG=""
# Path to JAR file (assuming it's in the target directory)
JAR_PATH="target/pattern-cache-1.0-SNAPSHOT.jar"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --method)
      METHOD="$2"
      shift 2
      ;;
    --initCacheAllocation)
      CACHE_ALLOCATION="$2"
      shift 2
      ;;
    --adaptation)
      ADAPTATION="true"
      shift 1
      ;;
    --cacheDb)
      CACHE_DB="true"
      shift 1
      ;;
    --type)
      TYPE="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --out)
      OUT_FOLDER="$2"
      shift 2
      ;;
    --measures)
      MEASURES="$2"
      shift 2
      ;;
    --schema)
      SCHEMA="$2"
      shift 2
      ;;
    --table)
      TABLE="$2"
      shift 2
      ;;
    --queries)
      QUERIES="$2"
      shift 2
      ;;
    --csv)
      CSV="$2"
      shift 2
      ;;
    --refinement)
      REFINEMENT="$2"
      shift 2
      ;;
    --agg)
      AGG="$2"
      shift 2
      ;;
    --relaxedCacheReuse)
      RELAXED_CACHE_REUSE="true"
      shift 1
      ;;
    --refinementScope)
      REFINEMENT_SCOPE="$2"
      shift 2
      ;;
    --maxRefinementSteps)
      MAX_REFINEMENT_STEPS="$2"
      shift 2
      ;;
    --calendarAlignment)
      CALENDAR_ALIGNMENT="$2"
      shift 2
      ;;
    --logBoundStats)
      LOG_BOUND_STATS="true"
      shift 1
      ;;
    --matchSelection)
      MATCH_SELECTION="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --type TYPE                System type (postgres, trino, duckdb)"
      echo "  --method METHOD            Fetched Aggregate method to use (ols, approxOls, m4)"
      echo "  --ground-truth METHOD      Generate ground truth using this method"
      echo "  --initCacheAllocation N    Initial cache allocation percentage (default: 0)"
      echo "  --mode MODE                Mode: timeCacheQueries, timeAggregateQueries, timeMatchRecognizeQueries"
      echo "  --runs N                   Number of runs"
      echo "  --out FOLDER               Output folder"
      echo "  --measures IDS             Measure IDs (space-separated)"
      echo "  --schema SCHEMA            Schema name"
      echo "  --table TABLE              Table name"
      echo "  --queries FILE             Path to queries file (required — generate via scripts/queries/generate_all.sh)"
      echo "  --csv PATH                 CSV path imported into DuckDB on connect (DuckDB only)"
      echo "  --cacheDb                  Materialize the CSV into a persistent DuckDB table (default: in-memory view, re-read per query)"
      echo "  --refinement STRATEGY      DOUBLING (default) or PREDICTED — adaptive refinement strategy"
      echo "  --agg N                    Initial aggregation factor (sub-buckets per pattern bucket)"
      echo "  --adaptation BOOL          Enable pattern adaptation for cache methods that support it (default false)"
      echo "  --refinementScope          'scoped' (default) | 'full' — executor for both visual + pattern: scoped per-region refinement vs one-shot full-range fetch"
      echo "  --maxRefinementSteps N     Scoped only: doubling steps before full-accuracy fallback (default 20 ≈ refine until cap)"
      echo "  --calendarAlignment BOOL   true (default): sub-bucket widths snap to calendar levels. false: raw-ms widths (MinMaxCache-style)"
      echo "  --help                     Display this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Check if JAR exists
if [ ! -f "$JAR_PATH" ]; then
  echo "JAR file not found at $JAR_PATH"
  echo "Please build the project first using: mvn clean package"
  exit 1
fi

# Build common arguments
COMMON_ARGS=(
  "-out" "$OUT_FOLDER"
  "-schema" "$SCHEMA"
  "-table" "$TABLE"
  "-runs" "$RUNS"
  "-measures" "$MEASURES"
  "-type" "$TYPE"
  "-initCacheAllocation" "$CACHE_ALLOCATION"
)

if [[ "$ADAPTATION" == "true" ]]; then
  COMMON_ARGS+=("-adaptation")
fi

# Add queries file if specified
if [ -n "$QUERIES" ]; then
  COMMON_ARGS+=("-queries" "$QUERIES")
fi

# DuckDB CSV path
if [ -n "$CSV" ]; then
  COMMON_ARGS+=("-csv" "$CSV")
fi

# DuckDB: cache the CSV into a persistent table instead of an in-memory view
if [[ "$CACHE_DB" == "true" ]]; then
  COMMON_ARGS+=("-cacheDb")
fi

# Refinement strategy + initial aggregation factor (only meaningful with -adaptation,
# but harmless to always pass — Experiments validates internally)
if [ -n "$REFINEMENT" ]; then
  COMMON_ARGS+=("-refinement" "$REFINEMENT")
fi
if [ -n "$AGG" ]; then
  COMMON_ARGS+=("-agg" "$AGG")
fi

# Relaxed cache reuse: pattern cache lookup admits cached sub-buckets that straddle
# the new query's outer-bucket grid (D2 shared-partial discipline).
if [[ "$RELAXED_CACHE_REUSE" == "true" ]]; then
  COMMON_ARGS+=("-relaxedCacheReuse")
fi

# Refinement scope (applies to both visual + pattern): 'full' (one fetch +
# full-range full-accuracy fallback) or 'scoped' (per-region refinement ladder
# + scoped full-accuracy fallback).
if [ -n "$REFINEMENT_SCOPE" ]; then
  COMMON_ARGS+=("-refinementScope" "$REFINEMENT_SCOPE")
fi
# Scoped only: cap on doubling steps before full-accuracy fallback fires.
if [ -n "$MAX_REFINEMENT_STEPS" ]; then
  COMMON_ARGS+=("-maxRefinementSteps" "$MAX_REFINEMENT_STEPS")
fi

# Calendar alignment toggle: true (default) snaps sub-buckets to calendar levels;
# false (MinMaxCache-style) uses raw-millisecond widths.
if [ -n "$CALENDAR_ALIGNMENT" ]; then
  COMMON_ARGS+=("-calendarAlignment" "$CALENDAR_ALIGNMENT")
fi

# Bound-stats logging for the D1/D2 experiment (per-sketch slope-interval dump).
if [[ "$LOG_BOUND_STATS" == "true" ]]; then
  COMMON_ARGS+=("-logBoundStats")
fi

# Matcher selection strategy: 'longest' (default) or 'all' (BFS, all valid matches).
if [ -n "$MATCH_SELECTION" ]; then
  COMMON_ARGS+=("-matchSelection" "$MATCH_SELECTION")
fi

# Function to run the experiment
run_experiment() {
  local exec_method=$1
  local experiment_mode=$2
  
  echo "Running experiment with method: $exec_method, mode: $experiment_mode"
  
  # Build arguments for this run
  local args=("${COMMON_ARGS[@]}" "-method" "$exec_method" "-mode" "$experiment_mode")
  echo "Args: ${args[@]}"

  # Execute the Java application
  java -Xmx4g -cp "$JAR_PATH" gr.imsi.athenarc.experiments.Experiments "${args[@]}"
  
  echo "Experiment completed: $exec_method, $experiment_mode"
  echo "----------------------------------------"
}

# Run the main experiment
run_experiment "$METHOD" "$MODE"
