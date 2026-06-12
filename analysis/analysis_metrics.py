"""VASTA analysis — the five paper experiments + their figures.

One per claim. Per-method accuracy and cost share the same data, so they
share one experiment with two plots; ρ_P / α / vis each vary a different
axis and stay separate.

  §5.2 — exp_method_summary    (claims 1 & 2: per method, accuracy + IO + time)
  §5.3 — exp_risk_coverage     (claim 3: bytes / F1 / observed error vs ρ_P)
  §5.4 — exp_bound_tightness   (claim 4: interval width + unbounded share vs α)
  §5.5 — exp_visual_summary    (claim 5: SSIM + pixel-diff vs uncached full-res)
  §5.6 — exp_cohort_summary    (claim 6: accuracy + IO across pattern-query
                                characteristics — length / angle-width / selectivity)

Usage:
    python analysis_metrics.py [--plot] [--save_csv]

Flags:
    --plot         write PDF + PNG figures to figures/
    --save_csv     write tables to figures/*.csv
    --cohort       which approxOls cohort feeds accuracy (default: approxOls)
    --tolerance_multiplier  boundary jitter in timeUnits (default: 1)
    --dataset / --table / --db / --folder   path + scope overrides
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from analysis_utils import (
    DATASET_ROWS,
    DEFAULT_DATASET,
    DEFAULT_DB,
    DEFAULT_FOLDER,
    collect_bound_stats,
    collect_cohort_metrics,
    collect_query_metrics,
    collect_risk_coverage,
    collect_timing,
    collect_visual_metrics,
)


OUT_DIR = Path("figures")
OUT_DIR.mkdir(exist_ok=True)


def set_output_subdir(name: str) -> None:
    """Pin CSV output under ``figures/<name>/``. Mirrors analysis_plots so
    every artifact from one analysis run lands in the same per-dataset dir."""
    global OUT_DIR
    OUT_DIR = Path("figures") / name
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def _csv_path(name: str) -> Path:
    return OUT_DIR / f"{name}.csv"


# ---------------------------------------------------------------------------
# Cohort filter — swaps a fallback-query's candidate metrics into the regular
# columns so every row reflects the approxOls path's standalone output. Without
# this, fallback queries score 1.0 trivially (the strict-OLS replay IS the GT)
# and hide approxOls's true accuracy.
# ---------------------------------------------------------------------------

_COHORT_SWAP_COLS = {
    "pred_count":          "candidate_pred_count",
    "true_positives":      "candidate_true_positives",
    "false_positives":     "candidate_false_positives",
    "false_negatives":     "candidate_false_negatives",
    "tp_confident":        "candidate_tp_confident",
    "tp_ambiguous":        "candidate_tp_ambiguous",
    "fp_confident":        "candidate_fp_confident",
    "fp_ambiguous":        "candidate_fp_ambiguous",
    "union_precision":     "candidate_union_precision",
    "recall":              "candidate_recall",
    "f1":                  "candidate_f1",
    "confident_precision": "candidate_confident_precision",
    "confident_recall":    "candidate_confident_recall",
}


def _apply_cohort_filter(df: pd.DataFrame, cohort: str) -> pd.DataFrame:
    if cohort == "all" or df.empty:
        return df
    if cohort == "fallback":
        if "fallback_triggered" not in df.columns:
            return df.iloc[0:0]
        return df[df["fallback_triggered"] == True]
    if cohort == "approxOls":
        if "candidate_true_positives" not in df.columns:
            return df
        df = df.copy()
        mask = df["candidate_true_positives"].notna()
        for dst, src in _COHORT_SWAP_COLS.items():
            if dst in df.columns and src in df.columns:
                df.loc[mask, dst] = df.loc[mask, src]
        return df
    raise ValueError(f"unknown cohort {cohort!r}")


def _normalize_dataset_col(df: pd.DataFrame) -> pd.DataFrame:
    """The raw CSV's ``dataset`` column duplicates ``dataset_label``; drop the
    former so pandas sees one 1-D ``dataset`` column for groupby."""
    if df.empty:
        return df
    if "dataset" in df.columns and "dataset_label" in df.columns:
        df = df.drop(columns=["dataset"])
    return df.rename(columns={"dataset_label": "dataset"})


def _print(df: pd.DataFrame) -> None:
    with pd.option_context("display.max_columns", None,
                           "display.width", 200,
                           "display.float_format", "{:.4f}".format):
        print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# §5.2 — per-method comparison: accuracy (P/R/F1 + bound pass-rate) on one
# axis, cost (total IO, total time, cache hits, peak bytes) on the other.
# Two plots draw from this table: plot_match_decomposition (accuracy detail)
# and plot_method_cost (IO + time + cumulative IO over the trace).
# ---------------------------------------------------------------------------

def _accuracy_per_method(df: pd.DataFrame) -> pd.DataFrame:
    """Per (dataset, method): macro + micro P/R/F1 + empirical bound-pass-rate.

    Macro = mean of per-query rates. Micro = rates over aggregated TP/FP/FN.
    bound_pass_rate = fraction of queries where union_precision ≥ 1−ρ_P
    (empirical sanity check, not a guarantee)."""
    if df.empty:
        return pd.DataFrame()
    macro_cols = ["union_precision", "confident_precision", "recall",
                  "confident_recall", "f1"]
    macro = (df.groupby(["dataset", "method"])[macro_cols]
               .mean().round(4).reset_index())
    sums = (df.groupby(["dataset", "method"])[
                ["true_positives", "false_positives", "false_negatives",
                 "tp_confident", "fp_confident"]].sum().reset_index())
    tp = sums["true_positives"]
    sums["micro_precision"] = tp / (tp + sums["false_positives"]).replace(0, np.nan)
    sums["micro_recall"]    = tp / (tp + sums["false_negatives"]).replace(0, np.nan)
    sums["micro_f1"] = (2 * sums["micro_precision"] * sums["micro_recall"] /
                        (sums["micro_precision"] + sums["micro_recall"]))
    sums = sums.round(4)
    out = macro.merge(
        sums[["dataset", "method", "micro_precision", "micro_recall", "micro_f1"]],
        on=["dataset", "method"])
    if "target_slack" in df.columns:
        b = df.dropna(subset=["target_slack"]).copy()
        if not b.empty:
            b["pass"] = b["union_precision"] >= (1.0 - b["target_slack"])
            pass_rate = (b.groupby(["dataset", "method"])["pass"]
                          .mean().round(4).rename("bound_pass_rate").reset_index())
            out = out.merge(pass_rate, on=["dataset", "method"], how="left")
    return out


def _cost_per_method(timing: pd.DataFrame) -> pd.DataFrame:
    """Per (dataset, method): IO + wall-clock + cache stats summed across the
    interleaved trace, averaged over runs.

    Sentinel -1 values written by methods that don't use the cache (OLS-NoC,
    MM+OLS-NoC's pat-side) are masked to NaN so sums/means stay meaningful."""
    if timing.empty:
        return pd.DataFrame()
    timing = timing.copy()
    for col in ("io_count", "cache_size_bytes", "cache_hits_pct"):
        if col in timing.columns:
            timing.loc[timing[col] < 0, col] = np.nan
    per_run = (timing.groupby(["dataset", "method", "run"])
                     .agg(total_time=("elapsed", "sum"),
                          mean_time=("elapsed", "mean"),
                          p95_time=("elapsed", lambda x: float(np.percentile(x, 95))),
                          total_io=("io_count", "sum"),
                          queries=("elapsed", "size"),
                          mean_cache_hits=("cache_hits_pct", "mean"),
                          peak_cache=("cache_size_bytes", "max"))
                     .reset_index())
    out = (per_run.groupby(["dataset", "method"])
                   .agg(runs=("run", "nunique"),
                        queries=("queries", "mean"),
                        total_io=("total_io", "mean"),
                        total_time=("total_time", "mean"),
                        mean_query_time=("mean_time", "mean"),
                        p95_query_time=("p95_time", "mean"),
                        mean_cache_hits_pct=("mean_cache_hits", "mean"),
                        peak_cache_bytes=("peak_cache", "max"))
                   .reset_index())
    return out


def exp_method_summary(qm_df: pd.DataFrame,
                       timing_df: pd.DataFrame,
                       save_csv: bool = False) -> pd.DataFrame:
    """Per (dataset, method) accuracy + cost row. One outer-join row per
    method — accuracy columns from per-query P/R/F1 (cohort-filtered upstream),
    cost columns from per-query timing summed over the trace.

    Self-contained methods + siloed composites land in the same table so the
    shared-vs-siloed contrast reads directly from adjacent rows."""
    print("\n=== §5.2 method summary (accuracy + cost) ===")
    acc = _accuracy_per_method(qm_df)
    cost = _cost_per_method(timing_df)
    if acc.empty and cost.empty:
        print("  (no rows; run scripts/experiments.sh methods first)")
        return pd.DataFrame()
    out = acc.merge(cost, on=["dataset", "method"], how="outer") \
             .sort_values(["dataset", "total_io"])
    _print(out.round(4))
    if save_csv:
        p = _csv_path("exp_method_summary")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# §5.3 — risk–coverage: bytes / F1 / observed error vs ρ_P (A3)
# ---------------------------------------------------------------------------

def exp_risk_coverage(table: str = "synth_10y_1m",
                      dataset: str = DEFAULT_DATASET,
                      db: str = DEFAULT_DB,
                      tolerance_multiplier: int = 1,
                      save_csv: bool = False) -> pd.DataFrame:
    """Per ρ_P (1 - target_accuracy): mean IO, mean F1, and observed
    bound-pass-rate over VASTA. Drives the trade-off curve in §5.4."""
    print("\n=== §5.3 risk-coverage (A3) ===")
    raw = collect_risk_coverage(table=table, dataset=dataset, db=db,
                                tolerance_multiplier=tolerance_multiplier)
    if raw.empty:
        print("  (no acc-suffixed runs; run scripts/experiments.sh risk first)")
        return pd.DataFrame()
    # Per ρ_P aggregate. IO/elapsed: sum over ALL queries (vis dominate the
    # absolute, pattern refinement drives the diff). F1 / precision / recall /
    # bound-satisfied: mean over PATTERN queries only (the ones with a GT and
    # therefore a union_precision value).
    raw["rho_p"] = 1.0 - raw["target_accuracy"]
    pat = raw.dropna(subset=["union_precision"]).copy()
    pat["bound_satisfied"] = pat["union_precision"] >= pat["target_accuracy"]
    # Per-query predicted-match count drives the match-weighted overshoot
    # metric — denominator is total matches (memory: decision-error metric).
    for c in ("tp_confident", "tp_ambiguous", "fp_confident", "fp_ambiguous"):
        if c not in pat.columns:
            pat[c] = 0
    pat[["tp_confident", "tp_ambiguous", "fp_confident", "fp_ambiguous"]] = (
        pat[["tp_confident", "tp_ambiguous", "fp_confident", "fp_ambiguous"]]
            .fillna(0).astype(int))
    pat["n_matches"] = (pat["tp_confident"] + pat["tp_ambiguous"]
                        + pat["fp_confident"] + pat["fp_ambiguous"])
    pat["err_x_n"] = pat["error_after"].fillna(0.0) * pat["n_matches"]
    totals = (raw.groupby(["target_accuracy", "rho_p"], as_index=False)
                 .agg(queries=("query_index", "size"),
                      total_io=("io_count", "sum"),
                      total_time=("elapsed", "sum")))
    acc = (pat.groupby(["target_accuracy", "rho_p"], as_index=False)
              .agg(pattern_queries=("query_index", "size"),
                   mean_f1=("f1", "mean"),
                   mean_precision=("union_precision", "mean"),
                   mean_recall=("recall", "mean"),
                   bound_pass_rate=("bound_satisfied", "mean"),
                   total_matches=("n_matches", "sum"),
                   sum_err_x_n=("err_x_n", "sum")))
    # Match-weighted mean overshoot: each match contributes its query's
    # error_after, denominator = total matches. Falls back to NaN when the
    # ρ_P slice produced zero matches.
    acc["mean_overshoot"] = (acc["sum_err_x_n"] /
                              acc["total_matches"].replace(0, np.nan))
    acc = acc.drop(columns=["sum_err_x_n"])
    out = totals.merge(acc, on=["target_accuracy", "rho_p"], how="left") \
                .sort_values("rho_p")
    _print(out.round(4))
    if save_csv:
        p = _csv_path("exp_risk_coverage")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# §5.4 — slope-bound tightness vs α (D1 width, D2 unbounded share)
# ---------------------------------------------------------------------------

def exp_bound_tightness(table: str = "synth_10y_1m",
                        dataset: str = DEFAULT_DATASET,
                        db: str = DEFAULT_DB,
                        save_csv: bool = False) -> pd.DataFrame:
    """Per α: mean/median/p95 interval width over evaluable sketches + the
    fraction in the unbounded regime (Q⁻ ≤ 0 or data-missing)."""
    print("\n=== §5.4 bound tightness (D1+D2) ===")
    rows = collect_bound_stats(table=table, dataset=dataset, db=db)
    if rows.empty:
        print("  (no bound_stats rows; run scripts/experiments.sh bound first)")
        return pd.DataFrame()
    eval_rows = rows[~rows["undefined"].fillna(True).astype(bool)]
    width = (eval_rows.groupby("alpha_sweep")["width_deg"]
                       .agg(mean="mean", median="median",
                            p95=lambda s: float(np.percentile(s, 95)),
                            n="size")
                       .reset_index())
    undef = (rows.groupby("alpha_sweep")["undefined"]
                  .agg(lambda s: float(s.fillna(True).mean()))
                  .reset_index().rename(columns={"undefined": "undefined_share"}))
    out = width.merge(undef, on="alpha_sweep", how="outer") \
               .sort_values("alpha_sweep")
    _print(out)
    if save_csv:
        p = _csv_path("exp_bound_tightness")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# §5.6 — characteristics sweep: per cohort (length × angle-width × selectivity)
# accuracy + IO. Cohort files live under queries/<table>/cohorts/ and are
# produced by scripts/queries/generate_all.sh; the runner is
# scripts/experiments.sh cohorts. Each cohort is a self-contained mini-workload
# (OLS GT + VASTA pass) so accuracy is comparable cohort-to-cohort.
# ---------------------------------------------------------------------------

_COHORT_BUCKET_ORDER = {
    "length":      ["2seg", "4seg", "8seg"],
    "width":       ["narrow", "medium", "wide"],
    "selectivity": ["many", "med", "few"],
}


def _cohort_sort_key(row: pd.Series) -> int:
    order = _COHORT_BUCKET_ORDER.get(row["cohort_axis"], [])
    return order.index(row["cohort_bucket"]) if row["cohort_bucket"] in order else 99


def exp_cohort_summary(table: str = "synth_10y_1m",
                       dataset: str = DEFAULT_DATASET,
                       db: str = DEFAULT_DB,
                       tolerance_multiplier: int = 1,
                       save_csv: bool = False) -> pd.DataFrame:
    """Per (cohort_axis, cohort_bucket): mean P/R/F1 + IO + wall-time over the
    cohort's pattern queries. One row per bucket; rows grouped by axis so the
    table reads as three side-by-side sub-tables (length, width, selectivity)."""
    print("\n=== §5.6 cohort sweep (length / width / selectivity) ===")
    df = collect_cohort_metrics(table=table, dataset=dataset, db=db,
                                tolerance_multiplier=tolerance_multiplier)
    if df.empty:
        print("  (no cohort runs; run scripts/experiments.sh cohorts first)")
        return pd.DataFrame()
    pat = df.dropna(subset=["union_precision"]).copy()
    agg_cols = {
        "queries":              ("query_index", "size"),
        "mean_union_precision": ("union_precision", "mean"),
        "mean_recall":          ("recall", "mean"),
        "macro_f1":             ("f1", "mean"),
        "sum_tp":               ("true_positives", "sum"),
        "sum_fp":               ("false_positives", "sum"),
        "sum_fn":               ("false_negatives", "sum"),
    }
    if "io_count" in df.columns:
        agg_cols["mean_io"] = ("io_count", "mean")
        agg_cols["total_io"] = ("io_count", "sum")
    if "elapsed" in df.columns:
        agg_cols["mean_time_ms"] = ("elapsed", "mean")
    out = (pat.groupby(["cohort_axis", "cohort_bucket"], as_index=False)
              .agg(**agg_cols))
    # Micro-F1: P/R/F1 computed from aggregated TP/FP/FN across the cohort.
    # Smoother than macro because every match contributes its own weight.
    tp = out["sum_tp"]
    out["micro_precision"] = tp / (tp + out["sum_fp"]).replace(0, np.nan)
    out["micro_recall"]    = tp / (tp + out["sum_fn"]).replace(0, np.nan)
    out["micro_f1"] = (2 * out["micro_precision"] * out["micro_recall"]
                       / (out["micro_precision"] + out["micro_recall"]))
    # Back-compat alias: existing callers reference mean_f1 (macro).
    out["mean_f1"] = out["macro_f1"]
    out["_order"] = out.apply(_cohort_sort_key, axis=1)
    out = out.sort_values(["cohort_axis", "_order"]).drop(columns="_order")
    _print(out.round(4))
    if save_csv:
        p = _csv_path("exp_cohort_summary")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# §5.5 — visual quality (SSIM + pixel-diff vs uncached full-res render)
# ---------------------------------------------------------------------------

def exp_visual_summary(visual_df: pd.DataFrame,
                       save_csv: bool = False) -> pd.DataFrame:
    """Per (dataset, method) mean/median SSIM + pixel-difference."""
    print("\n=== §5.5 visual quality ===")
    if visual_df is None or visual_df.empty:
        print("  (no visual metrics — run experiments.sh methods with NoC=1 first)")
        return pd.DataFrame()
    out = (visual_df.groupby(["dataset", "method"])
                    .agg(queries=("ssim", "size"),
                         mean_ssim=("ssim", "mean"),
                         median_ssim=("ssim", "median"),
                         mean_pixel_diff_pct=("pixel_diff_percentage", "mean"),
                         median_pixel_diff_pct=("pixel_diff_percentage", "median"))
                    .reset_index()
                    .sort_values(["dataset", "mean_ssim"],
                                 ascending=[True, False]))
    _print(out.round(4))
    if save_csv:
        p = _csv_path("exp_visual_summary")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# §5.8 — scalability: cumulative query time vs time-series length (rows)
# ---------------------------------------------------------------------------

def exp_scalability(datasets: List[str],
                    db: str = DEFAULT_DB,
                    save_csv: bool = False) -> pd.DataFrame:
    """Per (dataset, method): cumulative query time summed over the trace,
    paired with dataset row count. Methods/datasets without on-disk timing
    are silently skipped (so OLS-0 / OLS-C drop out at the larger scales).
    Siloed composites (name contains ``+`` or ``/``) are excluded — the
    sweep is about self-contained methods.

    Returns columns: dataset, n_rows, method, total_time.
    """
    print("\n=== §5.8 scalability (cumulative time vs rows) ===")
    rows: List[pd.DataFrame] = []
    for ds in datasets:
        timing = _normalize_dataset_col(
            collect_timing(folder=f"output/{ds}", dataset=ds, db=db,
                           table=ds, composites=[]))
        if timing.empty:
            print(f"  {ds}: no timing rows; skipping")
            continue
        cost = _cost_per_method(timing)[["dataset", "method", "total_time"]]
        cost["dataset_key"] = ds
        cost["n_rows"] = DATASET_ROWS.get(ds)
        rows.append(cost)
    if not rows:
        print("  (no datasets had timing — pass --scalability_datasets with run dirs that exist)")
        return pd.DataFrame()
    out = (pd.concat(rows, ignore_index=True)
             .dropna(subset=["n_rows"])
             .sort_values(["method", "n_rows"]))
    _print(out.round(4))
    if save_csv:
        p = _csv_path("exp_scalability")
        out.to_csv(p, index=False)
        print(f"  saved {p}")
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--folder",  default=DEFAULT_FOLDER)
    ap.add_argument("--dataset", default=DEFAULT_DATASET)
    ap.add_argument("--db",      default=DEFAULT_DB)
    ap.add_argument("--table",   default=None,
                    help="Workload name (default: --dataset).")
    ap.add_argument("--cohort", default="approxOls",
                    choices=("all", "approxOls", "fallback"))
    ap.add_argument("--tolerance_multiplier", type=int, default=1)
    ap.add_argument("--plot",     action="store_true")
    ap.add_argument("--save_csv", action="store_true")
    ap.add_argument("--scalability_datasets", default=None,
                    help="Comma-separated dataset list for the §5.8 scalability "
                         "sweep (e.g. synth_10y_1m,synth_10y_5s,synth_10y_1s). "
                         "When set, runs only the scalability driver.")
    ap.add_argument("--matcher", default="longest", choices=("longest", "all"),
                    help="Matcher path slot: 'longest' (default; reads/writes the canonical "
                         "locations) or 'all' (reads from output/<table>/<db>/matchall/..., "
                         "writes figures to figures/<dataset>/matchall/). Mirrors the "
                         "experiments.sh MATCH_SEL knob.")
    args = ap.parse_args()
    table = args.table or args.dataset
    # If --folder is left at its default but --dataset was supplied, derive
    # the folder so callers don't have to pass both. Matches the layout the
    # shell composes (output/<dataset>/<db>/<MethodLabel>/...).
    if args.folder == DEFAULT_FOLDER and args.dataset != DEFAULT_DATASET:
        args.folder = f"output/{args.dataset}"

    # When --matcher all, route I/O into the <db>/matchall subdir and figures
    # into figures/<dataset>/matchall/. Basenames stay clean (no _matchall
    # suffix) because the dataset label passed to plot_* is unchanged.
    out_subdir = args.dataset
    if args.matcher == "all":
        args.db = f"{args.db}/matchall"
        out_subdir = f"{args.dataset}/matchall"

    set_output_subdir(out_subdir)
    import analysis_plots
    analysis_plots.set_output_subdir(out_subdir)

    print(f"folder={args.folder}  dataset={args.dataset}  table={table}  "
          f"db={args.db}  cohort={args.cohort}")

    # §5.8 — scalability sweep is a cross-dataset experiment; the rest of the
    # pipeline is per-dataset, so we short-circuit when --scalability_datasets
    # is set and emit just the one figure into the primary dataset's dir.
    if args.scalability_datasets:
        ds_list = [d.strip() for d in args.scalability_datasets.split(",") if d.strip()]
        scale_df = exp_scalability(ds_list, db=args.db, save_csv=args.save_csv)
        if args.plot and not scale_df.empty:
            from analysis_plots import plot_scalability
            plot_scalability(scale_df)
        return

    # §5.2 — accuracy + cost rolled into one per-method table.
    qm_df = collect_query_metrics(
        folder=args.folder, dataset=args.dataset, db=args.db, table=table,
        tolerance_multiplier=args.tolerance_multiplier)
    qm_df = _apply_cohort_filter(qm_df, args.cohort)
    timing_df = _normalize_dataset_col(
        collect_timing(folder=args.folder, dataset=args.dataset, db=args.db,
                       table=table))
    method_df = exp_method_summary(qm_df, timing_df, save_csv=args.save_csv)

    # §5.3 (ρ_P sweep) + §5.4 (α sweep) + §5.5 (visual).
    risk_df = exp_risk_coverage(table=table, dataset=args.dataset, db=args.db,
                                tolerance_multiplier=args.tolerance_multiplier,
                                save_csv=args.save_csv)
    bound_df = exp_bound_tightness(table=table, dataset=args.dataset, db=args.db,
                                   save_csv=args.save_csv)
    vis_raw = collect_visual_metrics(
        folder=args.folder, dataset=args.dataset, db=args.db, table=table)
    exp_visual_summary(vis_raw, save_csv=args.save_csv)
    cohort_df = exp_cohort_summary(table=table, dataset=args.dataset, db=args.db,
                                   tolerance_multiplier=args.tolerance_multiplier,
                                   save_csv=args.save_csv)

    if not args.plot:
        return

    from analysis_plots import (
        plot_method_cost,
        plot_risk_coverage, plot_bound_tightness, plot_visual_metrics,
        plot_cohort_summary)

    if not method_df.empty:
        plot_method_cost(method_df, folder=args.folder, table=table,
                         dataset=args.dataset, db=args.db)
    if not risk_df.empty:
        plot_risk_coverage(risk_df)
    if not bound_df.empty:
        plot_bound_tightness(bound_df)
    if not vis_raw.empty:
        plot_visual_metrics(vis_raw)
    if not cohort_df.empty:
        plot_cohort_summary(cohort_df)


if __name__ == "__main__":
    main()
