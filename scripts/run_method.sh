#!/bin/bash

# Script to run pattern matching experiments with different methods
# This script executes the Experiments Java class with appropriate parameters

set -e  # Exit on error

# Default values for script parameters
METHOD="m4"
GROUND_TRUTH_METHOD=""  # Empty means no ground truth generation
MODE="timeCacheQueries"
RUNS=1
OUT_FOLDER="output"
CACHE_ALLOCATION=0
MEASURES="1"
SCHEMA="more"
TABLE="intel_lab_exp"
QUERIES=""
SEQ_COUNT=50
STATE_FILE="/Users/vasilisstamatopoulos/Documents/Works/ATHENA/PhD/Code/pattern-cache/config/pattern-hunter.properties"

# Path to JAR file (assuming it's in the target directory)
JAR_PATH="target/pattern-cache-1.0-SNAPSHOT-jar-with-dependencies.jar"

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
    --ground-truth)
      GROUND_TRUTH_METHOD="$2"
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
    --seq-count)
      SEQ_COUNT="$2"
      shift 2
      ;;
    --state-file)
      STATE_FILE="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --method METHOD           Method to use (m4, m4Inf, minmax)"
      echo "  --method PATTERN METHOD   Pattern method to use (firstLast, ols)"
      echo "  --ground-truth METHOD     Generate ground truth using this method"
      echo "  --initCacheAllocation N   Initial cache allocation percentage (default: 0)"
      echo "  --mode MODE               Mode: timeCacheQueries, timeQueries, generate"
      echo "  --runs N                  Number of runs"
      echo "  --out FOLDER              Output folder"
      echo "  --measures IDS            Measure IDs (space-separated)"
      echo "  --schema SCHEMA           Schema name"
      echo "  --table TABLE             Table name"
      echo "  --queries FILE            Path to queries file"
      echo "  --seq-count N             Number of queries in sequence"
      echo "  --state-file FILE         Path to state transitions file"
      echo "  --help                    Display this help message"
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
  "-seqCount" "$SEQ_COUNT"
  "-runs" "$RUNS"
  "-stateConfig" "$STATE_FILE"
  "-measures" "$MEASURES"
  "-initCacheAllocation" "$CACHE_ALLOCATION"
)

# Add queries file if specified
if [ -n "$QUERIES" ]; then
  COMMON_ARGS+=("-queries" "$QUERIES")
fi

# Function to run the experiment
run_experiment() {
  local exec_method=$1
  local experiment_mode=$2
  
  echo "Running experiment with method: $exec_method, mode: $experiment_mode"
  
  # Build arguments for this run
  local args=("${COMMON_ARGS[@]}" "-method" "$exec_method" "-mode" "$experiment_mode")
  
  # Execute the Java application
  java -Xmx4g -cp "$JAR_PATH" gr.imsi.athenarc.experiments.Experiments "${args[@]}"
  
  echo "Experiment completed: $exec_method, $experiment_mode"
  echo "----------------------------------------"
}

# If ground truth method is specified, generate it first
if [ -n "$GROUND_TRUTH_METHOD" ]; then
  echo "Generating ground truth with method: $GROUND_TRUTH_METHOD"
  run_experiment "$GROUND_TRUTH_METHOD" "timeQueries"
fi

# Run the main experiment
run_experiment "$METHOD" "$MODE"

# Compare results against ground truth if applicable
if [ -n "$GROUND_TRUTH_METHOD" ] && [ "$MODE" == "timeCacheQueries" ]; then
  echo "Comparing results between ground truth and test method"
  GROUND_TRUTH_DIR="$OUT_FOLDER/timeQueries/influx/$TABLE/run_0"
  TEST_DIR="$OUT_FOLDER/$MODE/influx/$TABLE/run_0"
  
  # Create directory for comparison reports
  COMPARISON_DIR="$OUT_FOLDER/comparisons"
  mkdir -p "$COMPARISON_DIR"

  # Find all pattern matches from ground truth and test method
  GROUND_TRUTH_PATTERN_FILES="$GROUND_TRUTH_DIR/pattern_matches/ground_truth/*.log"
  TEST_PATTERN_FILES="$TEST_DIR/pattern_matches/$METHOD/*.log"
  
  # Run comparison for all matching files
  for gt_file in $GROUND_TRUTH_PATTERN_FILES; do
    base_name=$(basename "$gt_file")
    test_file=$(find "$TEST_DIR/pattern_matches/$METHOD" -name "$base_name" -type f)
    
    if [ -n "$test_file" ] && [ -f "$test_file" ]; then
      echo "Comparing $base_name"
      java -cp "$JAR_PATH" gr.imsi.athenarc.middleware.pattern.PatternMatchComparator \
        "$gt_file" "$test_file" 0.8
    fi
  done
  
  echo "Comparisons completed. Results in $COMPARISON_DIR"
fi

echo "All pattern matching experiments completed"
