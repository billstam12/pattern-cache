"""
Shared utilities for VASTA accuracy analysis.

Match-file parsing, per-query refinement-stats CSV loading, dataclasses, and
the core precision/recall/F1 computation (with confident-only split that
validates bound rigor). Imported by ``analysis_metrics.py`` and
``analysis_plots.py``.
"""

from __future__ import annotations

import glob
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Default method registry
# ---------------------------------------------------------------------------

DEFAULT_DATASET = "synth_10y_5s"
DEFAULT_FOLDER = f"output/{DEFAULT_DATASET}"
DEFAULT_DB = "timescale"

# Maps the raw dataset folder name to the short label used in plots/tables.
DATASET_MAP: Dict[str, str] = {
    "intel_lab_exp": "INTL",
    "soccer_exp": "SOCC",
    "manufacturing_exp": "MNF",
    "spx": "SPX",
    "synth_10y_1m": "SNT5M",
    "synth_10y_5s": "SNT50M",
    "synth_10y_1s": "SNT250M",
    "synth_10y_316ms": "SNT1B",
}

# Row count for each synth dataset (10 years × sampling rate). Used by the
# §5.8 scalability sweep to put each dataset on the log-row x-axis.
DATASET_ROWS: Dict[str, int] = {
    "synth_10y_1m":   5_259_600,    # 10y × 1/min
    "synth_10y_5s":   63_115_200,   # 10y × 1/5s
    "synth_10y_1s":   315_576_000,  # 10y × 1/s
    "synth_10y_316ms": 998_658_228, # 10y × ~3.16/s
}

# Stable colours for plots — mirrors GLOBAL_METHOD_COLORS in the old notebook.
GLOBAL_METHOD_COLORS: Dict[str, str] = {
    "OLS-0":      "#1f77b4",
    "VASTA": "#9467bd",
    "OLS-C":      "#17becf",
    "Silo-0":     "#bcbd22",
    "Silo-MR":    "#e377c2",
}

# Publication font sizing — copy of the notebook's FONT_SIZE so plot dimensions
# survive the port unchanged.
FONT_SIZE: Dict = {
    "title": 22,
    "xlabel": 20,
    "ylabel": 20,
    "xtick": 20,
    "ytick": 20,
    "legend": 20,
    "figure": 12,
    "figsize": [16, 6],
}


# Pattern operation-type code → human label (used in operation breakdown plots).
def get_operation_type_mapping() -> Dict[str, str]:
    return {
        "P":   "Pan",
        "ZI":  "Zoom In",
        "ZO":  "Zoom Out",
        "R":   "Resize",
        "MC":  "Measure Change",
        "PD":  "Pattern Search",
        "NaN": "Initial Query",
    }


# (method_id, mode) → human-readable folder name. Mirrors method_label() in
# scripts/experiments.sh — both sides must agree since the shell composes the
# paths and the analysis reads them back.
_METHOD_LABELS: Dict[tuple, str] = {
    ("ols",       "timeCacheQueries"):           "OLS-C",
    ("approxOls", "timeCacheQueries"):           "VASTA",
    ("ols",       "timeAggregateQueries"):       "OLS-0",
    ("minmax",    "timeCacheQueries"):           "MinMaxCache",
    ("minMax",    "timeCacheQueries"):           "MinMaxCache",
    ("m4",        "timeCacheQueries"):           "M4",
    ("ols",       "timeMatchRecognizeQueries"):  "MR",
}


def method_label(method_id: str, mode: str) -> str:
    return _METHOD_LABELS.get((method_id, mode), f"{method_id}_{mode}")


def method_dir(dataset_folder: str, db: str, method_id: str, mode: str,
               variant: Optional[str] = None) -> str:
    """Per-invocation output dir composed exactly the same way the shell does:
        <dataset_folder>/<db>[/<variant>]/<MethodLabel>
    The shell mkdirs this path; Java writes ``run_<N>/results.csv`` and
    ``pattern_matches/`` inside it."""
    label = method_label(method_id, mode)
    parts = [dataset_folder, db]
    if variant:
        parts.append(variant)
    parts.append(label)
    return "/".join(parts)


@dataclass
class MethodSpec:
    """One method to evaluate against the OLS-NoC ground truth.

    ``base_folder`` is the per-method dir composed by the shell, e.g.
    ``output/synth_10y_1m/timescale/VASTA``. Java writes
    ``run_<N>/results.csv`` and ``pattern_matches/`` directly inside it."""
    name: str                 # display name (e.g. "VASTA")
    base_folder: str          # full per-method dir, e.g. "output/synth_10y_1m/timescale/VASTA"
    mode: str                 # "timeCacheQueries" or "timeAggregateQueries" — kept for row tagging
    method_id: str            # "approxOls" or "ols" — kept for row tagging
    database: str = DEFAULT_DB


def default_methods(folder: str = DEFAULT_FOLDER,
                    db: str = DEFAULT_DB) -> List[MethodSpec]:
    """Self-contained methods (each handles both view and pattern queries).

    ``folder`` is the dataset-level dir (e.g. ``output/synth_10y_1m``); each
    MethodSpec's ``base_folder`` is composed via ``method_dir()`` and points
    at the per-method subdir written by the shell.

    VASTA = paper's headline (min/max/count/sum, scoped refinement).
    OLS-C      = same cache layer + extra OLS regression sums per bucket.
    OLS-0      = no cache, exact OLS over raw data; accuracy floor + visual GT.
    """
    return [
        MethodSpec("OLS-0",      method_dir(folder, db, "ols",       "timeAggregateQueries"),
                   "timeAggregateQueries", "ols",       db),
        MethodSpec("OLS-C",      method_dir(folder, db, "ols",       "timeCacheQueries"),
                   "timeCacheQueries",     "ols",       db),
        MethodSpec("VASTA", method_dir(folder, db, "approxOls", "timeCacheQueries"),
                   "timeCacheQueries",     "approxOls", db),
    ]


# ---------------------------------------------------------------------------
# Siloed-composite specs (Fig. 1a — vis system + pattern system, no shared cache)
# Each composite is two MethodSpec-like locations whose timing CSVs are merged
# back into one interleaved trace via the split-mapping sidecar.
# ---------------------------------------------------------------------------

@dataclass
class SiloedComposite:
    """A two-process siloed stack producing one merged trace under ``name``.

    ``variant_folder`` is the per-variant dir (e.g.
    ``output/synth_10y_1m/timescale/siloed_mm_noc``); ``vis_spec``/``pat_spec``
    derive each subset's per-method dir inside it via ``method_dir()``. Each
    subset's results.csv carries query #s relative to its OWN file (vis-only
    or pattern-only); reconstruction of the original interleaved index uses
    the ``_split.csv`` written alongside the source queries.
    """
    name: str             # display label (e.g. "MM+OLS-NoC")
    variant_folder: str   # e.g. "output/synth_10y_1m/timescale/siloed_mm_noc"
    vis_mode: str         # e.g. "timeCacheQueries"
    vis_method_id: str    # e.g. "m4"
    pat_mode: str         # e.g. "timeAggregateQueries"
    pat_method_id: str    # e.g. "ols"
    database: str = DEFAULT_DB

    def _subset_folder(self, method_id: str, mode: str) -> str:
        # variant_folder already includes <dataset>/<db>/<variant>; just append the label.
        return f"{self.variant_folder}/{method_label(method_id, mode)}"

    def vis_spec(self) -> MethodSpec:
        return MethodSpec(self.name + "/vis",
                          self._subset_folder(self.vis_method_id, self.vis_mode),
                          self.vis_mode, self.vis_method_id, self.database)

    def pat_spec(self) -> MethodSpec:
        return MethodSpec(self.name + "/pat",
                          self._subset_folder(self.pat_method_id, self.pat_mode),
                          self.pat_mode, self.pat_method_id, self.database)


def default_siloed_composites(table: str = "synth_10y_1m",
                              db: str = DEFAULT_DB) -> List[SiloedComposite]:
    """Composites produced by scripts/experiments.sh siloed.

    ``MM+MR`` is opt-in (MR=1) so its folder may not exist; the loader
    silently skips it when its files aren't there. Silo-MR's pattern path
    requires MATCH_RECOGNIZE which only Trino implements, so its data
    always lands under ``output/<table>/trino/siloed_mm_mr/`` regardless of
    which backend served the canonical methods --- we pin the lookup here
    so the composite shows up in §5.2 alongside the timescale-based ones.
    """
    base = f"output/{table}/{db}"
    mr_base = f"output/{table}/trino"
    return [
        SiloedComposite("Silo-0",  f"{base}/siloed_mm_noc",
                        "timeCacheQueries",     "minmax",
                        "timeAggregateQueries", "ols", db),
        SiloedComposite("Silo-MR", f"{mr_base}/siloed_mm_mr",
                        "timeCacheQueries",        "minmax",
                        "timeMatchRecognizeQueries", "ols", "trino"),
    ]


def split_mapping_path(table: str) -> str:
    """Sidecar written by scripts/queries/split_queries.py — orig_idx,subset,subset_idx."""
    return os.path.join("queries", table, "queries_split.csv")


def collect_risk_coverage(table: str = "synth_10y_1m",
                          dataset: Optional[str] = None,
                          db: str = DEFAULT_DB,
                          accs: Optional[List[float]] = None,
                          tolerance_multiplier: int = 1) -> pd.DataFrame:
    """Per-(ρ_P, query) accuracy + timing rows from scripts/experiments.sh risk.

    Walks ``output/{table}/{db}/acc{ACC}/VASTA/`` for each requested ACC,
    loads VASTA's per-query timing and per-query P/R/F1, and concatenates
    them with a ``target_accuracy`` column. Composites are NOT included here —
    the sweep varies VASTA's tolerance, not the comparator set.
    """
    dataset = dataset or table
    accs = accs or [0.99, 0.95, 0.9, 0.8, 0.5]
    # GT (= ExactCache from the base methods run) lives outside the per-acc
    # dirs (which only produce VASTA output). Reuse that single GT
    # across the sweep.
    dataset_folder = f"output/{table}"
    gt_folder = method_dir(dataset_folder, db, "ols", "timeCacheQueries")
    rows: List[pd.DataFrame] = []
    for a in accs:
        folder = method_dir(dataset_folder, db, "approxOls", "timeCacheQueries",
                            variant=f"acc{a}")
        if not os.path.isdir(_resolve_folder(folder)):
            continue
        # Pass a single VASTA spec so collect_timing/collect_query_metrics
        # don't try to discover all default methods under this per-acc folder.
        sks_spec = MethodSpec("VASTA", folder, "timeCacheQueries", "approxOls", db)
        timing = collect_timing(folder=folder, dataset=dataset, db=db,
                                methods=[sks_spec], table=table, composites=[])
        accuracy = collect_query_metrics(folder=folder, dataset=dataset,
                                         db=db, table=table, composites=[],
                                         methods=[sks_spec],
                                         gt_folder=gt_folder,
                                         tolerance_multiplier=tolerance_multiplier)
        if timing.empty and accuracy.empty:
            continue
        if not timing.empty:
            # Keep ALL query rows (vis + pattern). Pattern queries hit cache
            # fully once vis warms it, so pattern-only rows would all show
            # io_count=0 and hide the ρ_P signal. The vis IO is constant
            # across the sweep; the diff comes from pattern refinement on top.
            timing = timing[timing["method"] == "VASTA"].copy()
            timing["target_accuracy"] = a
        if not accuracy.empty:
            accuracy = accuracy[accuracy["method"] == "VASTA"].copy()
            accuracy["target_accuracy"] = a
        # Left-join accuracy onto timing so EVERY query row stays; pattern
        # queries pick up their union_precision / f1 / target_slack, vis
        # queries get NaN for those (mean skips them).
        cols_t = ["method", "query_index", "io_count", "elapsed",
                  "target_accuracy", "run"]
        cols_a = ["query_index",
                  "union_precision", "recall", "f1", "target_slack",
                  "error_after",
                  "tp_confident", "tp_ambiguous",
                  "fp_confident", "fp_ambiguous"]
        t = timing[[c for c in cols_t if c in timing.columns]] \
            if not timing.empty else pd.DataFrame()
        a_df = accuracy[[c for c in cols_a if c in accuracy.columns]] \
            if not accuracy.empty else pd.DataFrame()
        if not t.empty and not a_df.empty:
            merged = t.merge(a_df, on="query_index", how="left")
        elif not t.empty:
            merged = t.copy()
        else:
            merged = a_df.copy()
            merged["target_accuracy"] = a
        merged["method"] = "VASTA"
        rows.append(merged)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def collect_bound_stats(table: str = "synth_10y_1m",
                        dataset: Optional[str] = None,
                        db: str = DEFAULT_DB,
                        alphas: Optional[List[int]] = None) -> pd.DataFrame:
    """Per-sketch slope-bound rows from scripts/run_bound_sweep.sh.

    The sweep writes one per-method dir per α:
    ``output/{table}/{db}/bound_a{α}/VASTA/bound_stats/run_*.csv`` with
    rows: query_index, agg_factor, sketch_idx, sketch_from_ms, sketch_to_ms,
    has_initialized, min_angle_deg, max_angle_deg, width_deg, undefined. This
    function concatenates them and tags the rows with the enclosing α so
    downstream code can group on it.
    """
    dataset = dataset or table
    alphas = alphas or [1, 2, 4, 8, 16, 32]
    dataset_folder = f"output/{table}"
    frames: List[pd.DataFrame] = []
    for a in alphas:
        per_method = method_dir(dataset_folder, db, "approxOls",
                                "timeCacheQueries", variant=f"bound_a{a}")
        base = _resolve_folder(per_method)
        glob_pat = os.path.join(base, "bound_stats", "run_*.csv")
        for path in sorted(glob.glob(glob_pat)):
            try:
                df = pd.read_csv(path)
            except Exception as e:
                print(f"  could not read {path}: {e}")
                continue
            if df.empty:
                continue
            run_name = os.path.basename(path).replace(".csv", "")
            df["run"] = int(run_name.split("_")[-1]) if "_" in run_name else 0
            df["alpha_sweep"] = a
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    for c in ("min_angle_deg", "max_angle_deg", "width_deg"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ("has_initialized", "undefined"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.lower().map(
                {"true": True, "false": False})
    return out



def collect_cohort_metrics(table: str = "synth_10y_1m",
                           dataset: Optional[str] = None,
                           db: str = DEFAULT_DB,
                           tolerance_multiplier: int = 1) -> pd.DataFrame:
    """Per-(cohort, query) accuracy + timing for the characteristics sweep.

    Each cohort file ``queries/{table}/cohorts/{axis}_{bucket}.txt`` is run
    through ``scripts/experiments.sh cohorts`` into its own variant directory
    ``output/{table}/{db}/cohort_{axis}_{bucket}/`` with subdirs ``ExactCache``
    (per-cohort ground truth) and ``VASTA`` (method under test). This
    function walks those dirs and tags every row with ``cohort``,
    ``cohort_axis``, ``cohort_bucket``.

    Only VASTA rows are returned — the OLS pass in each cohort dir is
    just the per-cohort ground truth and adds no per-method signal.
    """
    dataset = dataset or table
    cohort_dir = Path(_resolve_folder(str(Path("queries") / table / "cohorts")))
    if not cohort_dir.is_dir():
        return pd.DataFrame()
    dataset_folder = f"output/{table}"
    frames: List[pd.DataFrame] = []
    for cohort_file in sorted(cohort_dir.glob("*.txt")):
        name = cohort_file.stem
        axis, _, bucket = name.partition("_")
        variant = f"cohort_{name}"
        sks_folder = method_dir(dataset_folder, db, "approxOls",
                                "timeCacheQueries", variant=variant)
        gt_folder = method_dir(dataset_folder, db, "ols",
                               "timeCacheQueries", variant=variant)
        if not os.path.isdir(_resolve_folder(sks_folder)):
            continue
        sks_spec = MethodSpec("VASTA", sks_folder, "timeCacheQueries",
                              "approxOls", db)
        accuracy = collect_query_metrics(folder=sks_folder, dataset=dataset,
                                         db=db, table=table, composites=[],
                                         methods=[sks_spec],
                                         gt_folder=gt_folder,
                                         tolerance_multiplier=tolerance_multiplier)
        timing = collect_timing(folder=sks_folder, dataset=dataset, db=db,
                                methods=[sks_spec], table=table, composites=[])
        if accuracy.empty:
            continue
        accuracy = accuracy[accuracy["method"] == "VASTA"].copy()
        if not timing.empty:
            t = timing[timing["method"] == "VASTA"]
            # query_metrics doesn't carry "run", so average timing across runs.
            t = (t.groupby("query_index", as_index=False)[["io_count",
                                                            "elapsed"]].mean())
            accuracy = accuracy.merge(t, on="query_index", how="left")
        accuracy["cohort"] = name
        accuracy["cohort_axis"] = axis
        accuracy["cohort_bucket"] = bucket
        frames.append(accuracy)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# Ground truth comes from the OLS-NoC pass-through.
# GT_MODE = "timeAggregateQueries"
GT_MODE = "timeCacheQueries"
GT_METHOD_ID = "ols"

# Visual-similarity ground truth: the exact, no-cache, full-resolution render
# (the OLS-NoC / aggregate pass, useCache=false). Kept separate from GT_MODE on
# purpose. For visualization queries the rendered series depends only on the
# refinement scope, not on ols-vs-approxOls, so scoring a cached SCOPED render
# against another cached SCOPED render is comparing the same algorithm to
# itself — it is trivially identical (SSIM 1.0) and hides the real error. The
# only meaningful reference is the uncached full-resolution render the pixel
# error bound is defined against.
VISUAL_GT_MODE = "timeAggregateQueries"
VISUAL_GT_METHOD_ID = "ols"


def _resolve_folder(folder: str) -> str:
    """Resolve a relative output folder by checking cwd first, then one level up.

    Lets scripts run from either the project root or analysis/ without
    requiring callers to prefix paths with ``../`` manually. Absolute paths
    pass through unchanged.
    """
    if os.path.isabs(folder) or os.path.isdir(folder):
        return folder
    parent = os.path.join("..", folder)
    if os.path.isdir(parent):
        return parent
    return folder  # caller will hit the "not found" path with a clear error


def ground_truth_dir(folder: str, dataset: str = "", db: str = "") -> str:
    """``folder`` is now the per-method base dir (the GT method's). The
    ``dataset``/``db`` parameters are kept in the signature for backwards
    compatibility but are no longer used — both are encoded in ``folder``."""
    return os.path.join(_resolve_folder(folder), "pattern_matches")


def method_match_dir(spec: MethodSpec, dataset: str = "") -> str:
    return os.path.join(_resolve_folder(spec.base_folder), "pattern_matches")


def method_results_csv_glob(spec: MethodSpec, dataset: str = "") -> str:
    """results.csv may exist under multiple run_* dirs; caller concatenates."""
    return os.path.join(_resolve_folder(spec.base_folder),
                        "run_*", "results.csv")


# ---------------------------------------------------------------------------
# Match-file parsing
# ---------------------------------------------------------------------------

_MATCH_LINE_RE = re.compile(
    r"(Candidate)?(ConfidentMatch|AmbiguousMatch)\s*:\s*\[(\d+) to (\d+)\]")

# Match-file filenames carry the query index as a `q####` prefix.
_FILENAME_RE = re.compile(r"(?:q(\d+)_)?(.+?)_(\d+)_(\d+)_(.+?)\.log$")


@dataclass
class MatchFile:
    """A single ground-truth pattern-match log on disk."""
    path: str
    query_index: Optional[int]
    start_ts: str
    end_ts: str
    measure: str
    time_unit: str
    filename: str


def extract_patterns(path: str, candidate: bool = False) -> set:
    """Return {(start_ms, end_ms)} for the returned set (default) or the
    approxOls candidate set when ``candidate=True``."""
    with open(path) as f:
        content = f.read()
    return {(int(m.group(3)), int(m.group(4)))
            for m in _MATCH_LINE_RE.finditer(content)
            if bool(m.group(1)) == candidate}


def extract_patterns_with_tags(path: str,
                               candidate: bool = False) -> Dict[Tuple[int, int], str]:
    """Return {(start_ms, end_ms): 'confident' | 'ambiguous'} for the returned
    set (default) or the approxOls candidate set when ``candidate=True``.
    Ambiguous wins on key collision."""
    with open(path) as f:
        content = f.read()
    out: Dict[Tuple[int, int], str] = {}
    for m in _MATCH_LINE_RE.finditer(content):
        if bool(m.group(1)) != candidate:
            continue
        tag = "ambiguous" if m.group(2) == "AmbiguousMatch" else "confident"
        key = (int(m.group(3)), int(m.group(4)))
        if out.get(key) != "ambiguous":
            out[key] = tag
    return out


def find_ground_truth_files(folder: str, dataset: str, db: str) -> List[MatchFile]:
    """Discover OLS-NoC match-log files for one dataset."""
    pattern_dir = ground_truth_dir(folder, dataset, db)
    if not os.path.exists(pattern_dir):
        print(f"  GT dir not found: {pattern_dir}")
        return []
    out = []
    for path in sorted(glob.glob(os.path.join(pattern_dir, "*.log"))):
        m = _FILENAME_RE.match(os.path.basename(path))
        if not m:
            continue
        out.append(MatchFile(
            path=path,
            query_index=int(m.group(1)) if m.group(1) is not None else None,
            start_ts=m.group(2), end_ts=m.group(3),
            measure=m.group(4), time_unit=m.group(5),
            filename=os.path.basename(path),
        ))
    return out


def find_method_match_file(spec: MethodSpec, dataset: str,
                           gt: MatchFile) -> Optional[str]:
    """Locate the predicted-pattern file in ``spec`` corresponding to ``gt``."""
    base = f"{gt.start_ts}_{gt.end_ts}_{gt.measure}_{gt.time_unit}.log"
    candidates = []
    if gt.query_index is not None:
        candidates.append(os.path.join(method_match_dir(spec, dataset),
                                       f"q{gt.query_index:04d}_{base}"))
    candidates.append(os.path.join(method_match_dir(spec, dataset), base))
    for p in candidates:
        if os.path.exists(p):
            return p
    # Glob fallback for legacy naming drift.
    hits = glob.glob(os.path.join(method_match_dir(spec, dataset), f"q*_{base}"))
    return hits[0] if hits else None


# ---------------------------------------------------------------------------
# Per-query refinement-stats CSV (Experiments.java writes these)
# ---------------------------------------------------------------------------

_STATS_COLS_TO_NUM = ("accuracy", "errorBefore", "errorAfter")
_STATS_COLS_TO_INT = ("initialAggFactor", "finalAggFactor", "ambiguousAfter")
_STATS_COLS_TO_BOOL = ("refinementTriggered", "fallbackTriggered")


def load_stats_csv(spec: MethodSpec, dataset: str) -> pd.DataFrame:
    """Concatenate all run_*/results.csv files for one method; keep PD rows only.

    Returns an empty DataFrame if no CSV exists or it lacks the per-query
    pattern columns (pre-instrumentation runs). Column name normalised:
    `query #` → `query_index`.
    """
    frames = []
    for path in sorted(glob.glob(method_results_csv_glob(spec, dataset))):
        try:
            frames.append(pd.read_csv(path))
        except Exception as e:
            print(f"  could not read {path}: {e}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "query_type" in df.columns:
        df = df[df["query_type"].astype(str).str.upper().str.contains("PD|PATTERN", na=False)]
    if "query #" not in df.columns:
        return pd.DataFrame()
    df = df.rename(columns={"query #": "query_index"})
    for col in _STATS_COLS_TO_BOOL:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
    for col in _STATS_COLS_TO_INT:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _STATS_COLS_TO_NUM:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _normalize_results_csv(path: str, dataset_label: str) -> pd.DataFrame:
    """Read one results.csv and standardise columns + dtypes. Caller tags
    ``method`` / ``method_id`` / ``mode`` and (for composites) translates
    ``query_index`` from subset-index to original interleaved order."""
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"  could not read {path}: {e}")
        return pd.DataFrame()
    run_dir = os.path.basename(os.path.dirname(path))
    run_num = int(run_dir.split("_")[-1]) if run_dir.startswith("run_") else 0
    df = df.rename(columns={
        "query #":               "query_index",
        "Time (sec)":            "elapsed",
        "Init Time (sec)":       "init_time",
        "IO Count":              "io_count",
        "Cache Hits (%)":        "cache_hits_pct",
        "Cache Size (bytes)":    "cache_size_bytes",
    })
    df["run"] = run_num
    df["dataset_label"] = dataset_label
    for col in ("elapsed", "init_time", "cache_hits_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("io_count", "cache_size_bytes", "query_index"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _load_method_timing(spec: MethodSpec, dataset: str,
                        dataset_label: str) -> pd.DataFrame:
    """Self-contained method: one process per run, query_index is already the
    original interleaved index."""
    frames = []
    for path in sorted(glob.glob(method_results_csv_glob(spec, dataset))):
        df = _normalize_results_csv(path, dataset_label)
        if df.empty:
            continue
        df["method"] = spec.name
        df["method_id"] = spec.method_id
        df["mode"] = spec.mode
        df["subset"] = "all"
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_composite_timing(composite: "SiloedComposite", table: str,
                           dataset: str, dataset_label: str) -> pd.DataFrame:
    """Siloed composite: two processes per run (vis + pat); ``query_index`` is
    translated from subset-index to original interleaved index via
    ``queries/<table>/queries_split.csv``."""
    split_path = split_mapping_path(table)
    if not os.path.exists(split_path):
        return pd.DataFrame()
    split_df = pd.read_csv(split_path)

    def _load_subset(spec: MethodSpec, subset_label: str) -> pd.DataFrame:
        subset_map = split_df[split_df["subset"] == subset_label]
        if subset_map.empty:
            return pd.DataFrame()
        idx_to_orig = dict(zip(subset_map["subset_idx"].astype(int),
                               subset_map["orig_idx"].astype(int)))
        frames = []
        for path in sorted(glob.glob(method_results_csv_glob(spec, dataset))):
            df = _normalize_results_csv(path, dataset_label)
            if df.empty:
                continue
            df["subset_idx"] = df["query_index"]
            df["query_index"] = df["subset_idx"].map(idx_to_orig).astype("Int64")
            df = df.dropna(subset=["query_index"])
            df["method"] = composite.name
            df["method_id"] = spec.method_id
            df["mode"] = spec.mode
            df["subset"] = subset_label
            frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    parts = [_load_subset(composite.vis_spec(), "vis"),
             _load_subset(composite.pat_spec(), "pat")]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def collect_timing(folder: str = DEFAULT_FOLDER,
                   dataset: str = DEFAULT_DATASET,
                   db: str = DEFAULT_DB,
                   methods: Optional[List[MethodSpec]] = None,
                   composites: Optional[List["SiloedComposite"]] = None,
                   table: Optional[str] = None,
                   ) -> pd.DataFrame:
    """Long-form timing table for self-contained methods AND siloed composites.

    Returns one row per query × run × method (× subset for composites) with
    columns: dataset_label, method, method_id, mode, subset, run,
    query_index, query_type, init_time, elapsed, io_count, cache_hits_pct,
    cache_size_bytes, accuracy, initialAggFactor, finalAggFactor,
    refinementTriggered, fallbackTriggered, errorBefore, errorAfter,
    ambiguousAfter.

    Composites are loaded when their output dirs exist on disk and the split
    mapping is present; missing ones are silently skipped. The PD-only filter
    is NOT applied here — callers downstream filter ``query_type`` themselves.
    """
    methods = methods or default_methods(folder, db)
    if composites is None:
        composites = default_siloed_composites(table=table or dataset, db=db)
    dataset_label = DATASET_MAP.get(dataset, dataset)
    frames = []
    for spec in methods:
        df = _load_method_timing(spec, dataset, dataset_label)
        if not df.empty:
            frames.append(df)
    for comp in composites:
        df = _load_composite_timing(comp, table or dataset, dataset, dataset_label)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def lookup_query_stats(stats_df: pd.DataFrame, query_index: Optional[int]) -> Dict:
    """Get the stats row for one query, or empty dict."""
    if query_index is None or stats_df.empty or "query_index" not in stats_df.columns:
        return {}
    sub = stats_df[stats_df["query_index"] == query_index]
    if sub.empty:
        return {}
    row = sub.iloc[0]
    out = {}
    for col in (*_STATS_COLS_TO_NUM, *_STATS_COLS_TO_INT, *_STATS_COLS_TO_BOOL):
        if col in sub.columns:
            out[col] = row.get(col)
    return out


# ---------------------------------------------------------------------------
# Tolerance + pattern matching
# ---------------------------------------------------------------------------

_AGG_INTERVAL_RE = re.compile(r"AggregateInterval\{\s*(\d+)\s+(\w+)\s*\}")
_UNIT_TO_MS = {
    "millis": 1, "milliseconds": 1,
    "seconds": 1_000,
    "minutes": 60_000,
    "hours":   3_600_000,
    "days":    86_400_000,
}


def get_tolerance_ms(time_unit: str, multiplier: int = 1) -> int:
    """Return tolerance for boundary matching in milliseconds.

    Default is `multiplier × timeUnit`. `multiplier=1` absorbs the 1-bucket
    LONGEST-selection boundary jitter that otherwise makes ConfidentMatch
    precision dip below 1.0 even when the bound is rigorous. Set to 0 for
    exact-match scoring.
    """
    if multiplier <= 0:
        return 0
    m = _AGG_INTERVAL_RE.search(time_unit or "")
    if not m:
        return 0
    value = int(m.group(1))
    unit = m.group(2).lower()
    factor = next((ms for k, ms in _UNIT_TO_MS.items() if k in unit), None)
    if factor is None:
        return 0
    return value * factor * multiplier


def find_best_matching_patterns(gt: set, pred: set, tolerance_ms: int):
    """Greedy best-match: pair (gt, pred) with both endpoints within tolerance,
    minimum total endpoint-difference first. Returns
    ``(matched_pairs, unmatched_gt, unmatched_pred)``.
    """
    gt_list = list(gt)
    pred_list = list(pred)
    candidates = []
    for i, g in enumerate(gt_list):
        for j, p in enumerate(pred_list):
            ds = abs(g[0] - p[0]); de = abs(g[1] - p[1])
            if ds <= tolerance_ms and de <= tolerance_ms:
                candidates.append((ds + de, i, j, g, p, ds, de))
    candidates.sort(key=lambda x: x[0])
    used_g: set = set(); used_p: set = set()
    matched = []
    for total, i, j, g, p, ds, de in candidates:
        if i in used_g or j in used_p:
            continue
        matched.append({"gt": g, "pred": p, "start_diff": ds, "end_diff": de})
        used_g.add(i); used_p.add(j)
    unmatched_gt = [gt_list[i] for i in range(len(gt_list)) if i not in used_g]
    unmatched_pred = [pred_list[j] for j in range(len(pred_list)) if j not in used_p]
    return matched, unmatched_gt, unmatched_pred


# ---------------------------------------------------------------------------
# Core metric: precision / recall / F1 with confident split
# ---------------------------------------------------------------------------

@dataclass
class QueryMetrics:
    """One row per (method, dataset, query_index)."""
    dataset: str
    method: str
    method_id: str
    measure: str
    time_unit: str
    query_index: Optional[int]
    tolerance_ms: int
    gt_count: int
    pred_count: int
    true_positives: int
    false_positives: int
    false_negatives: int
    tp_confident: int
    tp_ambiguous: int
    fp_confident: int
    fp_ambiguous: int
    union_precision: float
    recall: float
    f1: float
    confident_precision: float
    confident_recall: float
    accuracy: Optional[float] = None
    target_slack: Optional[float] = None
    bound_holds: Optional[bool] = None
    initial_agg_factor: Optional[int] = None
    final_agg_factor: Optional[int] = None
    refinement_triggered: Optional[bool] = None
    fallback_triggered: Optional[bool] = None
    error_before: Optional[float] = None
    error_after: Optional[float] = None
    ambiguous_after: Optional[int] = None
    candidate_pred_count: Optional[int] = None
    candidate_true_positives: Optional[int] = None
    candidate_false_positives: Optional[int] = None
    candidate_false_negatives: Optional[int] = None
    candidate_tp_confident: Optional[int] = None
    candidate_tp_ambiguous: Optional[int] = None
    candidate_fp_confident: Optional[int] = None
    candidate_fp_ambiguous: Optional[int] = None
    candidate_union_precision: Optional[float] = None
    candidate_recall: Optional[float] = None
    candidate_f1: Optional[float] = None
    candidate_confident_precision: Optional[float] = None
    candidate_confident_recall: Optional[float] = None


def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den > 0 else default


def _score_against_gt(gt_patterns: set, pred_patterns: set,
                      pred_tags: Dict[Tuple[int, int], str],
                      tolerance_ms: int) -> Dict[str, float]:
    matched, unmatched_gt, unmatched_pred = find_best_matching_patterns(
        gt_patterns, pred_patterns, tolerance_ms)
    tp = len(matched)
    fp = len(unmatched_pred)
    fn = len(unmatched_gt)
    tp_conf = sum(1 for m in matched if pred_tags.get(tuple(m["pred"])) == "confident")
    tp_amb  = tp - tp_conf
    fp_conf = sum(1 for p in unmatched_pred if pred_tags.get(tuple(p)) == "confident")
    fp_amb  = sum(1 for p in unmatched_pred if pred_tags.get(tuple(p)) == "ambiguous")
    union_p = _safe_div(tp, tp + fp, default=1.0 if (tp + fn) == 0 else 0.0)
    recall  = _safe_div(tp, tp + fn, default=1.0 if (tp + fp) == 0 else 0.0)
    f1      = _safe_div(2 * union_p * recall, union_p + recall)
    conf_p  = _safe_div(tp_conf, tp_conf + fp_conf, default=1.0)
    conf_r  = _safe_div(tp_conf, tp + fn,           default=1.0)
    return {
        "pred_count": len(pred_patterns),
        "tp": tp, "fp": fp, "fn": fn,
        "tp_conf": tp_conf, "tp_amb": tp_amb,
        "fp_conf": fp_conf, "fp_amb": fp_amb,
        "union_precision": union_p, "recall": recall, "f1": f1,
        "confident_precision": conf_p, "confident_recall": conf_r,
    }


def compute_query_metrics(spec: MethodSpec, dataset_label: str,
                          gt: MatchFile, pred_path: str,
                          tolerance_ms: int,
                          stats_row: Optional[Dict] = None) -> QueryMetrics:
    """Compute precision/recall/F1 + confident split + stats join for one query.

    Scores the returned set against GT. When the pred file also carries an
    approxOls candidate block (fallback queries), scores that against GT too
    and populates the ``candidate_*`` columns."""
    gt_patterns = extract_patterns(gt.path)
    pred_patterns = extract_patterns(pred_path)
    pred_tags = extract_patterns_with_tags(pred_path)
    r = _score_against_gt(gt_patterns, pred_patterns, pred_tags, tolerance_ms)

    cand_patterns = extract_patterns(pred_path, candidate=True)
    cand: Optional[Dict[str, float]] = None
    if cand_patterns:
        cand_tags = extract_patterns_with_tags(pred_path, candidate=True)
        cand = _score_against_gt(gt_patterns, cand_patterns, cand_tags, tolerance_ms)

    stats = stats_row or {}
    acc = stats.get("accuracy")
    slack = None
    bound_holds = None
    if acc is not None and not (isinstance(acc, float) and np.isnan(acc)):
        slack = 1.0 - float(acc)
        bound_holds = r["union_precision"] >= (1.0 - slack)

    return QueryMetrics(
        dataset=dataset_label, method=spec.name, method_id=spec.method_id,
        measure=gt.measure, time_unit=gt.time_unit, query_index=gt.query_index,
        tolerance_ms=tolerance_ms,
        gt_count=len(gt_patterns), pred_count=r["pred_count"],
        true_positives=r["tp"], false_positives=r["fp"], false_negatives=r["fn"],
        tp_confident=r["tp_conf"], tp_ambiguous=r["tp_amb"],
        fp_confident=r["fp_conf"], fp_ambiguous=r["fp_amb"],
        union_precision=r["union_precision"], recall=r["recall"], f1=r["f1"],
        confident_precision=r["confident_precision"],
        confident_recall=r["confident_recall"],
        accuracy=float(acc) if acc is not None and not (isinstance(acc, float) and np.isnan(acc)) else None,
        target_slack=slack, bound_holds=bound_holds,
        initial_agg_factor=_to_opt_int(stats.get("initialAggFactor")),
        final_agg_factor=_to_opt_int(stats.get("finalAggFactor")),
        refinement_triggered=_to_opt_bool(stats.get("refinementTriggered")),
        fallback_triggered=_to_opt_bool(stats.get("fallbackTriggered")),
        error_before=_to_opt_float(stats.get("errorBefore")),
        error_after=_to_opt_float(stats.get("errorAfter")),
        ambiguous_after=_to_opt_int(stats.get("ambiguousAfter")),
        candidate_pred_count        = cand["pred_count"]        if cand else None,
        candidate_true_positives    = cand["tp"]                if cand else None,
        candidate_false_positives   = cand["fp"]                if cand else None,
        candidate_false_negatives   = cand["fn"]                if cand else None,
        candidate_tp_confident      = cand["tp_conf"]           if cand else None,
        candidate_tp_ambiguous      = cand["tp_amb"]            if cand else None,
        candidate_fp_confident      = cand["fp_conf"]           if cand else None,
        candidate_fp_ambiguous      = cand["fp_amb"]            if cand else None,
        candidate_union_precision   = cand["union_precision"]   if cand else None,
        candidate_recall            = cand["recall"]            if cand else None,
        candidate_f1                = cand["f1"]                if cand else None,
        candidate_confident_precision = cand["confident_precision"] if cand else None,
        candidate_confident_recall  = cand["confident_recall"]  if cand else None,
    )


def _to_opt_int(v) -> Optional[int]:
    if v is None: return None
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_opt_float(v) -> Optional[float]:
    if v is None: return None
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_opt_bool(v) -> Optional[bool]:
    if v is None: return None
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return v
    return bool(v)


# ---------------------------------------------------------------------------
# Convenience: run the full sweep for one (folder, dataset, methods)
# ---------------------------------------------------------------------------

def collect_query_metrics(folder: str = DEFAULT_FOLDER,
                          dataset: str = DEFAULT_DATASET,
                          db: str = DEFAULT_DB,
                          methods: Optional[List[MethodSpec]] = None,
                          composites: Optional[List["SiloedComposite"]] = None,
                          table: Optional[str] = None,
                          gt_folder: Optional[str] = None,
                          tolerance_multiplier: int = 1,
                          verbose: bool = False) -> pd.DataFrame:
    """Walk every (method, query) pair and build a long-form DataFrame.

    Includes self-contained methods AND siloed composites. For a composite,
    pattern queries are scored against its pat-side spec (e.g. OLS-NoC or
    MATCH_RECOGNIZE output). Match files are located by the existing
    ``q*_{base}.log`` glob fallback so no extra index translation is needed.

    ``tolerance_multiplier=1`` allows 1-bucket boundary jitter (recommended;
    isolates bound-rigor failures from LONGEST-selection alignment).
    Set to 0 for exact-match scoring.
    """
    methods = methods or default_methods(folder, db)
    if composites is None:
        composites = default_siloed_composites(table=table or dataset, db=db)
    dataset_label = DATASET_MAP.get(dataset, dataset)
    # GT pattern_matches live under the GT method's per-method dir, not under
    # the dataset folder root. Default to ExactCache (GT_MODE/GT_METHOD_ID).
    if gt_folder is None:
        gt_folder = method_dir(folder, db, GT_METHOD_ID, GT_MODE)
    gt_files = find_ground_truth_files(gt_folder, dataset, db)
    if not gt_files:
        return pd.DataFrame()

    # (display_name, spec_for_file_lookup, refinement_stats_available)
    entries: List[Tuple[str, MethodSpec, bool]] = [(m.name, m, True) for m in methods]
    for comp in composites:
        entries.append((comp.name, comp.pat_spec(), False))

    rows: List[QueryMetrics] = []
    for display_name, spec, has_stats in entries:
        stats_df = load_stats_csv(spec, dataset) if has_stats else pd.DataFrame()
        for gt in gt_files:
            pred_path = find_method_match_file(spec, dataset, gt)
            if not pred_path:
                if verbose:
                    print(f"  no pred for {display_name} q{gt.query_index}")
                continue
            tol = get_tolerance_ms(gt.time_unit, multiplier=tolerance_multiplier)
            stats_row = lookup_query_stats(stats_df, gt.query_index) if has_stats else {}
            try:
                qm = compute_query_metrics(spec, dataset_label, gt, pred_path, tol, stats_row)
                qm.method = display_name  # override spec.name for composites
            except Exception as e:
                print(f"  error scoring {display_name} q{gt.query_index}: {e}")
                continue
            rows.append(qm)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([r.__dict__ for r in rows])


# ---------------------------------------------------------------------------
# Visual-similarity metrics (visualization queries)
#
# Pattern-detection queries are scored by precision/recall/F1 above. The other
# query types (initial / pan / zoom / resize / measure-change) are *rendered*
# results: their accuracy is how closely a method's chart reproduces the OLS
# ground-truth chart. We render both series to PNGs with cairo_plot and score
# them with SSIM + pixel-difference, producing a per-query frame that mirrors
# the QueryMetrics one so the two accuracy notions sit side by side.
# ---------------------------------------------------------------------------

# query_type codes/labels that mean "pattern detection" (scored elsewhere).
def _is_pattern_query_type(query_type) -> bool:
    s = str(query_type).upper()
    return "PD" in s or "PATTERN" in s


def method_data_dir(spec: MethodSpec, dataset: str = "") -> str:
    """Root of one method's per-query result CSVs (run_*/query_*/*.csv).
    Same as ``spec.base_folder`` in the post-refactor layout — kept as a
    named helper since callers append ``run_N/query_<idx>/0.csv``."""
    return _resolve_folder(spec.base_folder)


def query_series_csv(spec: MethodSpec, dataset: str, run: str,
                     query_index: int) -> str:
    """The rendered-series CSV a query wrote (one ``timestamp,<measure>`` file)."""
    return os.path.join(method_data_dir(spec, dataset), run,
                        f"query_{query_index}", "0.csv")


def ground_truth_spec(folder: str = DEFAULT_FOLDER, db: str = DEFAULT_DB) -> MethodSpec:
    """The exact, no-cache OLS render that visual results are scored against
    (the OLS-NoC pass). Addressed as a method so its per-query result CSVs
    resolve. Deliberately pinned to ``VISUAL_GT_MODE`` rather than the cached
    path: a cached same-scope render is not a valid reference (it equals the
    method under test by construction)."""
    return MethodSpec(name="GT",
                      base_folder=method_dir(folder, db,
                                             VISUAL_GT_METHOD_ID, VISUAL_GT_MODE),
                      mode=VISUAL_GT_MODE,
                      method_id=VISUAL_GT_METHOD_ID, database=db)


def _ts_to_epoch_ms(v) -> Optional[int]:
    """Normalise a results.csv `from`/`to` cell (pandas Timestamp, epoch number,
    or ISO string) to epoch milliseconds, treating naive times as UTC."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return int(v.value // 1_000_000)
    if isinstance(v, (int, float)):
        return int(v if v >= 1e12 else v * 1000)
    try:
        return int(pd.to_datetime(v, utc=True).value // 1_000_000)
    except Exception:
        return None


def _resolve_gt_series_csv(gt_spec: MethodSpec, dataset: str, run: str,
                           query_index: int) -> Optional[str]:
    """GT render for one query, preferring the same run then any run (OLS is
    deterministic, so the render is identical across runs)."""
    same = query_series_csv(gt_spec, dataset, run, query_index)
    if os.path.exists(same):
        return same
    hits = glob.glob(os.path.join(method_data_dir(gt_spec, dataset),
                                  "run_*", f"query_{query_index}", "0.csv"))
    return hits[0] if hits else None


def _render_series_to_png(csv_path: Optional[str], out_noext: str,
                          width: int, height: int,
                          q_from: Optional[int], q_to: Optional[int]) -> Optional[str]:
    """Render a ``timestamp,<measure>`` CSV to ``out_noext.png`` via cairo_plot.

    Returns the PNG path, or None when the CSV is missing/empty/unplottable.
    """
    from cairo_plot import plot  # lazy: keeps the pattern path import-free
    if not csv_path or not os.path.exists(csv_path):
        return None
    df = pd.read_csv(csv_path)
    if df.empty or "timestamp" not in df.columns:
        return None
    measures = [c for c in df.columns if c != "timestamp"]
    if not measures:
        return None
    measure = measures[0]
    df = df.dropna(subset=["timestamp", measure]).sort_values("timestamp")
    if len(df) < 2:
        return None
    # cairo_plot normalises y by (max-min); nudge a flat series so it renders.
    if df[measure].max() == df[measure].min():
        df = df.copy()
        df.iloc[-1, df.columns.get_loc(measure)] += 1e-9
    if q_from is None or q_to is None or q_to <= q_from:
        q_from = int(df["timestamp"].min())
        q_to = int(df["timestamp"].max())
    # cairo_plot's pixelInterval = (to-from)//width must be >= 1.
    if q_to - q_from < width:
        return None
    plot(df, measure, out_noext, width, height, q_from, q_to)
    return out_noext + ".png"


@dataclass
class VisualQueryMetrics:
    """One row per (method, dataset, run, visualization-query)."""
    dataset: str
    method: str
    method_id: str
    run: str
    query_index: int
    query_type: str
    operation: str
    width: int
    height: int
    ssim: float
    pixel_diff_percentage: float
    target_accuracy: Optional[float] = None


def collect_visual_metrics(folder: str = DEFAULT_FOLDER,
                           dataset: str = DEFAULT_DATASET,
                           db: str = DEFAULT_DB,
                           methods: Optional[List[MethodSpec]] = None,
                           composites: Optional[List["SiloedComposite"]] = None,
                           table: Optional[str] = None,
                           verbose: bool = False) -> pd.DataFrame:
    """Per visualization-query SSIM + pixel-difference vs the OLS render.

    Includes self-contained methods AND siloed composites. For a composite,
    view queries are scored against its vis-side spec; the subset_idx in the
    composite's results.csv is translated to the original interleaved index
    via the split mapping so the GT render lookup (which uses orig_idx) lines
    up.

    Returns an empty DataFrame if the rendering stack (cairo_plot) is
    unavailable or no visualization queries produced result CSVs.
    """
    try:
        from cairo_plot import compute_ssim, compute_pixel_difference_percentage
    except Exception as e:  # cairo / skimage / PIL missing
        print(f"  visual metrics unavailable (cannot import cairo_plot: {e})")
        return pd.DataFrame()

    methods = methods or default_methods(folder, db)
    if composites is None:
        composites = default_siloed_composites(table=table or dataset, db=db)
    dataset_label = DATASET_MAP.get(dataset, dataset)
    gt_spec = ground_truth_spec(folder, db)
    op_map = get_operation_type_mapping()

    # The reference must be the exact no-cache render. If it is absent we skip
    # rather than fall back to a cached render — scoring against a cached
    # same-scope render would report a meaningless SSIM of 1.0.
    gt_dir = method_data_dir(gt_spec, dataset)
    if not os.path.isdir(gt_dir):
        print(f"  visual ground truth not found: {gt_dir}")
        print(f"  (expected the no-cache full-resolution {VISUAL_GT_METHOD_ID} "
              f"render — run the OLS-NoC pass). Skipping visual metrics.")
        return pd.DataFrame()

    # Translation for composites: vis-subset_idx → orig_idx.
    vis_subset_translation: Dict[str, Dict[int, int]] = {}
    if composites:
        split_path = split_mapping_path(table or dataset)
        if os.path.exists(split_path):
            split_df = pd.read_csv(split_path)
            vis_map = split_df[split_df["subset"] == "vis"]
            mapping = dict(zip(vis_map["subset_idx"].astype(int),
                               vis_map["orig_idx"].astype(int)))
            for c in composites:
                vis_subset_translation[c.name] = mapping

    # (display_name, spec_for_data, subset_idx→orig_idx mapping or None)
    entries: List[Tuple[str, MethodSpec, Optional[Dict[int, int]]]] = \
        [(m.name, m, None) for m in methods]
    for comp in composites:
        entries.append((comp.name, comp.vis_spec(),
                        vis_subset_translation.get(comp.name)))

    def _load_raw_results(spec: MethodSpec) -> pd.DataFrame:
        # Used only here — keeps the `query #` column (collect_timing renames
        # it to `query_index`) and tags rows with the run-dir name so we can
        # find the matching per-query CSV under run_*/query_*/.
        paths = sorted(glob.glob(method_results_csv_glob(spec, dataset)))
        if not paths:
            return pd.DataFrame()
        frames = []
        for p in paths:
            df = pd.read_csv(p)
            df["run"] = os.path.basename(os.path.dirname(p))
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    tmp_dir = tempfile.mkdtemp(prefix="visual_metrics_")
    gt_png_cache: Dict[int, Optional[str]] = {}
    rows: List[VisualQueryMetrics] = []
    try:
        for display_name, spec, translation in entries:
            res = _load_raw_results(spec)
            if res.empty or "query #" not in res.columns:
                continue
            for _, r in res.iterrows():
                qt = r.get("query_type")
                if _is_pattern_query_type(qt):
                    continue
                raw_idx = int(r["query #"])
                # For composites raw_idx is the vis-subset index; the GT
                # render lives at orig_idx. Translate when a mapping is
                # provided; skip the row if the index is unmapped (shouldn't
                # happen on well-formed runs).
                if translation is not None:
                    orig_idx = translation.get(raw_idx)
                    if orig_idx is None:
                        continue
                else:
                    orig_idx = raw_idx
                run = str(r.get("run", "run_0"))
                width = int(r.get("width") or 1000)
                height = int(r.get("height") or 600)
                q_from = _ts_to_epoch_ms(r.get("from"))
                q_to = _ts_to_epoch_ms(r.get("to"))

                method_csv = query_series_csv(spec, dataset, run, raw_idx)
                try:
                    method_png = _render_series_to_png(
                        method_csv, os.path.join(tmp_dir, f"{display_name}_{run}_{raw_idx}"),
                        width, height, q_from, q_to)
                    if orig_idx not in gt_png_cache:
                        gt_csv = _resolve_gt_series_csv(gt_spec, dataset, run, orig_idx)
                        gt_png_cache[orig_idx] = _render_series_to_png(
                            gt_csv, os.path.join(tmp_dir, f"gt_{orig_idx}"),
                            width, height, q_from, q_to)
                    gt_png = gt_png_cache[orig_idx]
                    if method_png is None or gt_png is None:
                        if verbose:
                            print(f"  no render for {display_name} {run} q{raw_idx}")
                        continue
                    ssim = float(compute_ssim(method_png, gt_png))
                    pix = float(compute_pixel_difference_percentage(method_png, gt_png))
                except Exception as e:
                    print(f"  error rendering {display_name} {run} q{raw_idx}: {e}")
                    continue

                rows.append(VisualQueryMetrics(
                    dataset=dataset_label, method=display_name, method_id=spec.method_id,
                    run=run, query_index=orig_idx, query_type=str(qt),
                    operation=op_map.get(str(qt), str(qt)),
                    width=width, height=height,
                    ssim=ssim, pixel_diff_percentage=pix,
                    target_accuracy=_to_opt_float(r.get("accuracy")),
                ))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([r.__dict__ for r in rows])


