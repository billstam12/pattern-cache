#!/bin/bash

# Single dispatcher for all VASTA experiments.
#
# Subcommands:
#   methods    Self-contained methods (ExactCache, VASTA). Set NoC=1 to add OLS-NoC
#              (slow, but required for the visual-SSIM ground truth).
#   siloed     Siloed composites for C1 (MM+OLS-NoC). Set MR=1 to add MM+MATCH_RECOGNIZE
#              (requires DB=trino — Postgres/Timescale don't implement MATCH_RECOGNIZE).
#   bound      α-sweep for D1+D2 bound-tightness. ALPHAS=... overrides the sweep list.
#   risk       ρ_P-sweep for A3 risk-coverage. ACCS=... overrides the accuracy list.
#   cohorts    Characteristics sweep (§5.6) — runs VASTA + OLS GT on each cohort
#              file under queries/<table>/cohorts/. Cohort files come from
#              scripts/queries/generate_all.sh.
#   ablation   VASTA with calendarAlignment=false (the no-cal ablation).
#   analysis   python analysis/analysis_metrics.py --all --plot
#   all        methods (NoC=1) + siloed + bound + ablation + analysis
#
# Environment knobs (all optional):
#   TABLE=synth_10y_1m   Workload + dataset name (default: synth_10y_1m).
#   DB=timescale         Backend: timescale|postgres|trino|duckdb (default: timescale).
#   USE_REMOTE=1         Default: talk to the shared lab server (pulsar) using the
#                        read-only guest account; datasets are preloaded there.
#                        Set USE_REMOTE=0 to bring up a local Docker container.
#   RUNS=5               Runs per method.
#   AGG=4                Initial aggregation factor for VASTA.
#   NoC=1                Add OLS-NoC pass under 'methods' / 'all'.
#   MR=1                 Add MM+MATCH_RECOGNIZE composite under 'siloed' / 'all'.
#   ALPHAS="1 2 4 8 16 32"  α list for 'bound' / 'all'.
#   ACCS="0.99 0.95 0.9 0.8 0.5"  ρ_P=1-acc list for 'risk' / 'all'.
#   MATCH_SEL=all        Pattern matcher: 'longest' (canonical, omit) or 'all'
#                        (BFS enumerates every valid match — the recall
#                        attribution sweep for §5.3). When set, every output
#                        path is nested under a 'matchall' subdir of <db> and
#                        '--matchSelection $MATCH_SEL' is forwarded to Java,
#                        so the matchall runs sit alongside the canonical
#                        ones without overwriting them.

set -e

TABLE="${TABLE:-synth_10y_1m}"
DB="${DB:-timescale}"
RUNS="${RUNS:-5}"
AGG="${AGG:-4}"
MATCH_SEL="${MATCH_SEL:-}"
# Default to the shared lab server (the JVM-side properties point at pulsar,
# whose datasets are preloaded and reachable with the read-only guest account).
# Set USE_REMOTE=0 to bring up a local Docker container instead.
USE_REMOTE="${USE_REMOTE:-1}"
DATA_DIR="${DATA_DIR:-/opt/exp-data}"
DATA="${DATA_DIR}/${TABLE}.csv"
QUERIES="queries/${TABLE}/queries.txt"
# Java now receives a per-invocation output folder that the shell composes
# and mkdirs. The structure is:
#   output/<dataset>/<db>[/matchall]/[<variant>/]<MethodLabel>/run_<N>/
# where <variant> is omitted for the base "methods" run and present for
# acc<A>, bound_a<α>, cohort_<name>, siloed_mm_noc, siloed_mm_mr, nocal.
# The 'matchall' tier is inserted only when MATCH_SEL is set.
OUT_BASE="output/${TABLE}/${DB}${MATCH_SEL:+/matchall}"
EXTRA_ARGS=()
if [[ -n "$MATCH_SEL" ]]; then
  EXTRA_ARGS+=(--matchSelection "$MATCH_SEL")
fi

# Map (methodId, mode) → human-readable folder name. Same labels the analysis
# notebook uses in figures (analysis_utils.py:GLOBAL_METHOD_COLORS).
method_label() {
  local methodId="$1" mode="$2"
  case "${methodId}_${mode}" in
    ols_timeCacheQueries)              echo "OLS-C" ;;
    approxOls_timeCacheQueries)        echo "VASTA" ;;
    ols_timeAggregateQueries)          echo "OLS-0" ;;
    minmax_timeCacheQueries|minMax_timeCacheQueries) echo "MinMaxCache" ;;
    m4_timeCacheQueries)               echo "M4" ;;
    ols_timeMatchRecognizeQueries)     echo "MR" ;;
    *) echo "${methodId}_${mode}" ;;  # fallback for new combinations
  esac
}

# Compose + mkdir a per-invocation output folder and echo it.
#   compose_out <methodId> <mode> [<variant>]
# Variant is optional; when omitted (base methods run) the path is
#   $OUT_BASE/<label>; when set it becomes $OUT_BASE/<variant>/<label>.
compose_out() {
  local label
  label=$(method_label "$1" "$2")
  local path
  if [[ $# -ge 3 && -n "$3" ]]; then
    path="$OUT_BASE/$3/$label"
  else
    path="$OUT_BASE/$label"
  fi
  mkdir -p "$path"
  echo "$path"
}

# Fired only after a valid subcommand is selected (deferred so usage doesn't
# spin up a DB container). Default is remote (USE_REMOTE=1): the JVM-side
# properties point at the shared lab server (pulsar). Set USE_REMOTE=0 to bring
# up a local Docker container and fall back to it instead.
start_backend() {
  if [[ "$USE_REMOTE" == "0" ]]; then
    case "$DB" in
      postgres)  ./scripts/postgres/start.sh ;;
      timescale) ./scripts/timescale/start.sh ;;
    esac
  fi
  case "$DB" in
    postgres|timescale) EXTRA_ARGS+=(--schema public) ;;
  esac
  set -x
}

# ---------------------------------------------------------------------------
# methods — self-contained methods that handle the full interleaved trace.
# ---------------------------------------------------------------------------
cmd_methods() {
  # ExactCache (cached exact OLS — stores all five regression sums per bucket).
  sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
     --method ols --mode timeCacheQueries --runs "$RUNS" \
     --queries "$QUERIES" --out "$(compose_out ols timeCacheQueries)"

  # VASTA (the paper's headline — min/max/count/sum, scoped refinement).
  sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
     --method approxOls --mode timeCacheQueries --runs "$RUNS" \
     --queries "$QUERIES" --out "$(compose_out approxOls timeCacheQueries)" \
     --adaptation --agg "$AGG" --refinementScope scoped

  # OLS-NoC — no cache, exact OLS over raw data. Slow; opt-in via NoC=1.
  # Required for the visual-SSIM ground truth (uncached full-res render).
  if [[ -n "${NoC:-}" ]]; then
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method ols --mode timeAggregateQueries --runs "$RUNS" \
       --queries "$QUERIES" --out "$(compose_out ols timeAggregateQueries)"
  fi
}

# ---------------------------------------------------------------------------
# siloed — C1: two-process composites with no state shared across paths.
# ---------------------------------------------------------------------------
cmd_siloed() {
  local Q_VIS="queries/${TABLE}/queries_vis.txt"
  local Q_PAT="queries/${TABLE}/queries_pat.txt"
  # Split is normally written by scripts/queries/generate_all.sh; only fall back
  # to live-splitting when the sidecar files are missing.
  if [[ ! -f "$Q_VIS" || ! -f "$Q_PAT" ]]; then
    python scripts/queries/split_queries.py "$QUERIES"
  fi

  # MM+OLS-NoC: MinMaxCache (minmax + calendarAlignment=false) for vis, OLS-NoC for pattern.
  sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
     --method minmax --mode timeCacheQueries --runs "$RUNS" \
     --queries "$Q_VIS" --out "$(compose_out minmax timeCacheQueries siloed_mm_noc)" \
     --calendarAlignment false
  sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
     --method ols --mode timeAggregateQueries --runs "$RUNS" \
     --queries "$Q_PAT" --out "$(compose_out ols timeAggregateQueries siloed_mm_noc)"

  # MM+MR: same vis stack, MATCH_RECOGNIZE for pattern. Opt-in via MR=1.
  # MATCH_RECOGNIZE requires DB=trino — Postgres/Timescale don't implement it
  # (Trino connection details in src/main/resources/application.properties).
  if [[ -n "${MR:-}" ]]; then
    if [[ "$DB" != "trino" ]]; then
      echo "experiments.sh siloed: MR=1 requires DB=trino (got DB=$DB)." >&2
      echo "  re-run as: DB=trino MR=1 sh ./scripts/experiments.sh siloed" >&2
      exit 2
    fi
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method m4 --mode timeCacheQueries --runs "$RUNS" \
       --queries "$Q_VIS" --out "$(compose_out minmax timeCacheQueries siloed_mm_mr)" \
       --calendarAlignment false
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method ols --mode timeMatchRecognizeQueries --runs "$RUNS" \
       --queries "$Q_PAT" --out "$(compose_out ols timeMatchRecognizeQueries siloed_mm_mr)"
  fi
}

# ---------------------------------------------------------------------------
# bound — D1+D2: per-sketch slope-interval dump across an α sweep.
# ---------------------------------------------------------------------------
cmd_bound() {
  local SWEEP="${ALPHAS:-1 2 4 8 16 32}"
  for A in $SWEEP; do
    # --adaptation is required: PatternDataProcessor.computeSubInterval uses
    # divider=4 when adaptation is off, masking the α effect we want to plot.
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method approxOls --mode timeCacheQueries --runs "$RUNS" \
       --queries "$QUERIES" --out "$(compose_out approxOls timeCacheQueries "bound_a${A}")" \
       --adaptation --agg "$A" --refinementScope scoped --logBoundStats
  done
}

# ---------------------------------------------------------------------------
# risk — A3: ρ_P sweep. Rewrites the queries file's accuracy column (col 8)
#        for each requested value and runs VASTA into a suffixed dir.
# ---------------------------------------------------------------------------
cmd_risk() {
  local SWEEP="${ACCS:-0.99 0.95 0.9 0.8 0.5}"
  for A in $SWEEP; do
    local QFILE="queries/${TABLE}/queries_acc${A}.txt"
    # Acc variants are normally written by scripts/queries/generate_all.sh;
    # only fall back to live-rewriting when the file is missing.
    if [[ ! -f "$QFILE" ]]; then
      awk -v a="$A" 'BEGIN{FS=OFS=","} {$8=a; print}' "$QUERIES" > "$QFILE"
    fi
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method approxOls --mode timeCacheQueries --runs "$RUNS" \
       --queries "$QFILE" --out "$(compose_out approxOls timeCacheQueries "acc${A}")" \
       --adaptation --agg "$AGG" --refinementScope scoped
  done
}

# ---------------------------------------------------------------------------
# cohorts — §5.6 characteristics sweep: one self-contained mini-workload per
#           cohort file. OLS pass acts as per-cohort ground truth so accuracy
#           is comparable across cohorts; VASTA pass is the method under
#           test. Cohort files come from scripts/queries/generate_all.sh.
# ---------------------------------------------------------------------------
cmd_cohorts() {
  local COHORT_DIR="queries/${TABLE}/cohorts"
  if [[ ! -d "$COHORT_DIR" ]]; then
    echo "experiments.sh cohorts: $COHORT_DIR not found — run" >&2
    echo "  scripts/queries/generate_all.sh $TABLE" >&2
    exit 2
  fi
  shopt -s nullglob
  local FILES=("$COHORT_DIR"/*.txt)
  shopt -u nullglob
  if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "experiments.sh cohorts: no .txt files in $COHORT_DIR" >&2
    exit 2
  fi
  for COHORT_FILE in "${FILES[@]}"; do
    local NAME=$(basename "$COHORT_FILE" .txt)
    local VARIANT="cohort_${NAME}"
    # Per-cohort ground truth.
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method ols --mode timeCacheQueries --runs "$RUNS" \
       --queries "$COHORT_FILE" --out "$(compose_out ols timeCacheQueries "$VARIANT")"
    # VASTA (the method under test).
    sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
       --method approxOls --mode timeCacheQueries --runs "$RUNS" \
       --queries "$COHORT_FILE" --out "$(compose_out approxOls timeCacheQueries "$VARIANT")" \
       --adaptation --agg "$AGG" --refinementScope scoped
  done
}

# ---------------------------------------------------------------------------
# ablation — VASTA without calendar alignment (raw-ms widths).
# ---------------------------------------------------------------------------
cmd_ablation() {
  sh ./scripts/run_method.sh --type "$DB" --csv "$DATA" --table "$TABLE" "${EXTRA_ARGS[@]}" \
     --method approxOls --mode timeCacheQueries --runs "$RUNS" \
     --queries "$QUERIES" --out "$(compose_out approxOls timeCacheQueries nocal)" \
     --adaptation --agg "$AGG" --refinementScope scoped \
     --calendarAlignment false
}

# ---------------------------------------------------------------------------
# analysis — produce every table + figure under figures/.
# ---------------------------------------------------------------------------
cmd_analysis() {
  local MATCHER_ARGS=()
  [[ -n "$MATCH_SEL" ]] && MATCHER_ARGS+=(--matcher "$MATCH_SEL")
  python analysis/analysis_metrics.py --plot --save_csv \
     --dataset "$TABLE" --table "$TABLE" --db "$DB" "${MATCHER_ARGS[@]}"
}

cmd_all() {
  NoC=1 cmd_methods
  cmd_siloed
  cmd_bound
  cmd_risk
  cmd_ablation
  cmd_analysis
}

case "${1:-}" in
  methods)  start_backend; cmd_methods  ;;
  siloed)   start_backend; cmd_siloed   ;;
  bound)    start_backend; cmd_bound    ;;
  risk)     start_backend; cmd_risk     ;;
  cohorts)  start_backend; cmd_cohorts  ;;
  ablation) start_backend; cmd_ablation ;;
  analysis)                cmd_analysis ;;  # python-only, no DB needed
  all)      start_backend; cmd_all      ;;
  *)
    cat <<EOF
Usage: $0 {methods|siloed|bound|risk|cohorts|ablation|analysis|all}

  methods    ExactCache + VASTA (and OLS-NoC when NoC=1)
  siloed     MM+OLS-NoC (and MM+MR when MR=1, requires DB=trino)
  bound      α-sweep for D1+D2 bound-tightness
  risk       ρ_P-sweep for A3 risk-coverage
  cohorts    §5.6 characteristics sweep (length/width/selectivity)
  ablation   VASTA with calendarAlignment=false
  analysis   python analysis/analysis_metrics.py --plot
  all        methods (NoC=1) + siloed + bound + risk + ablation + analysis

Environment: TABLE=$TABLE  DB=$DB  RUNS=$RUNS  AGG=$AGG  USE_REMOTE=$USE_REMOTE
             USE_REMOTE=1 (default; shared lab server)  USE_REMOTE=0 (local Docker)
             NoC=1 (adds OLS-NoC)  MR=1 (adds MATCH_RECOGNIZE, needs DB=trino)
             MATCH_SEL=all (nest under <db>/matchall, --matchSelection all)
             ALPHAS="..." (override α sweep)  ACCS="..." (override ρ_P sweep)
EOF
    exit 1
    ;;
esac
