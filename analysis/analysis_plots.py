"""Publication-quality plots — IEEE 2-column conference layout.

Each chart is a single-axes figure at "half-column" size (≈1.7" wide), so
four figures fit across the two-column page.

Output filenames follow ``fig_<section>_<topic>[_<view>].(pdf|png)``.
``<section>`` mirrors the paper §5.X subsection so files sort by paper
order; ``<view>`` discriminates between datasets / methods when several
variants exist.

    §5.2 cost          fig_5_2_memory_<dataset>
                       fig_5_2_time_<dataset>
    §5.3 risk          fig_5_3_f1_vs_rho
                       fig_5_3_bound_vs_rho        (per-query SLO pass)
                       fig_5_3_overshoot_vs_rho    (match-weighted)
                       fig_5_3_time_vs_rho         (total query time + IO)
    §5.4 bound         fig_5_4_width_vs_alpha
                       fig_5_4_unbounded_vs_alpha
    §5.5 visual        fig_5_5_ssim_<dataset>
                       fig_5_5_pixdiff_<dataset>
    §5.6 cohorts       fig_5_6_<axis>_macroF1
                       fig_5_6_<axis>_microF1
                       (axis ∈ {length, width, selectivity})

Inputs are passed in as DataFrames from :mod:`analysis_metrics`; no I/O on
the read side.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from analysis_utils import (
    DEFAULT_DB,
    DEFAULT_FOLDER,
    GLOBAL_METHOD_COLORS,
    collect_timing,
)


PLOT_DIR = Path("figures")
PLOT_DIR.mkdir(exist_ok=True)


def set_output_subdir(name: str) -> None:
    """Pin plot output under ``figures/<name>/``. Idempotent — call once at the
    start of an analysis run with the dataset name so figures from different
    datasets don't overwrite each other."""
    global PLOT_DIR
    PLOT_DIR = Path("figures") / name
    PLOT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# IEEE 2-column conference style — applied process-wide on import.
# ---------------------------------------------------------------------------

# Two figures per IEEE column ⇒ ~1.7" wide; we use 1.75×1.45 in for a
# slightly taller-than-square aspect that leaves room for axis ticks.
HALFCOL_FIGSIZE: Tuple[float, float] = (1.75, 1.45)

# Single-column charts (richer content: stacked bars, multi-line trace).
COL_FIGSIZE: Tuple[float, float] = (3.5, 2.35)

plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["Times New Roman", "Times", "STIX", "DejaVu Serif"],
    "mathtext.fontset": "stix",
    "font.size": 8,
    "axes.titlesize": 8,
    "axes.labelsize": 8,
    "axes.labelweight": "normal",
    "axes.titleweight": "normal",
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
    "legend.frameon": False,
    "legend.handlelength": 1.5,
    "legend.handletextpad": 0.5,
    "legend.columnspacing": 1.0,
    "legend.borderpad": 0.3,
    "axes.grid": True,
    "axes.axisbelow": True,
    "grid.alpha": 0.35,
    "grid.linewidth": 0.4,
    "grid.linestyle": ":",
    "axes.linewidth": 0.7,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "xtick.major.size": 2.5,
    "ytick.major.size": 2.5,
    "lines.linewidth": 1.3,
    "lines.markersize": 3.5,
    "patch.linewidth": 0.6,
    "figure.dpi": 150,
    "savefig.dpi": 600,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.02,
    "savefig.transparent": False,
    "pdf.fonttype": 42,  # editable text in the PDF (no Type 3 outlines)
    "ps.fonttype": 42,
    "figure.constrained_layout.use": True,
})


# ---------------------------------------------------------------------------
# Palette — print-safe categorical. Method colors reuse the canonical set
# defined in analysis_utils so legends are consistent across plots.
# ---------------------------------------------------------------------------

# Bar accents for the match-decomposition figure.
_C_TP_CONF = "#2a7f3e"
_C_TP_AMB  = "#a8d8a0"
_C_FN      = "#7d7d7d"
_C_FP_CONF = "#b02c2c"
_C_FP_AMB  = "#f4a6a3"

# Single-series accents.
_C_PRIMARY    = "#3556a5"   # blue   — bytes, width
_C_SECONDARY  = "#d1632c"   # orange — p95 overlay, F1 secondary
_C_TERTIARY   = "#2a7f3e"   # green  — recall overlay
_C_WARN       = "#b02c2c"   # red    — bound, unbounded share

_DATASET_LABEL_FALLBACK = "all"


def _save(fig: plt.Figure, name: str) -> Path:
    base = PLOT_DIR / name
    fig.savefig(f"{base}.pdf")
    fig.savefig(f"{base}.png", dpi=600)
    print(f"  saved {base}.pdf")
    plt.close(fig)
    return base


def _new_fig(figsize: Tuple[float, float] = HALFCOL_FIGSIZE
             ) -> Tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    return fig, ax


def _safe(label: str) -> str:
    """Filename-safe view discriminator (dataset name, method name, ...)."""
    return (label.replace("/", "_").replace(" ", "_")
                 .replace("+", "p").replace("(", "").replace(")", ""))


def _method_order(df: pd.DataFrame) -> List[str]:
    """Self-contained methods first, composites after; within each group,
    preserve df order so callers can sort by a chosen metric."""
    preferred = ["OLS-0", "OLS-C", "VASTA"]
    present = list(df["method"].unique())
    ordered = [m for m in preferred if m in present]
    ordered += [m for m in present if m not in ordered]
    return ordered


_SHORT_METHOD = {
    "OLS-0":      "OLS-0",
    "OLS-C":      "OLS-C",
    "VASTA": "VASTA",
    "Silo-0":     "Silo-0",
    "Silo-MR":    "Silo-MR",
}


def _short_method(name: str) -> str:
    """Half-column-friendly method label. The full names collide at 1.75",
    so we always abbreviate the canonical three; composites are returned
    as-is (callers can rotate ticks if they get long)."""
    if name in _SHORT_METHOD:
        return _SHORT_METHOD[name]
    return name


# ---------------------------------------------------------------------------
# §5.2 cost — three single-chart figures per dataset: total IO, total time,
# cumulative IO over the trace. Total-IO/total-time at half-column; the
# cumulative trace gets one column because it has multiple lines.
# ---------------------------------------------------------------------------

def _ordered_method_subframe(sub: pd.DataFrame,
                             sort_key: str = "total_io") -> pd.DataFrame:
    """Order rows: self-contained methods (no '+') asc by sort_key, then
    composites asc by sort_key."""
    is_comp = sub["method"].str.contains(r"\+", regex=True)
    ordered = (sub[~is_comp].sort_values(sort_key)["method"].tolist()
               + sub[is_comp].sort_values(sort_key)["method"].tolist())
    return sub.set_index("method").loc[ordered].reset_index()


def _hbar(ax: plt.Axes, methods: List[str], values: np.ndarray,
          xlabel: str) -> None:
    """Horizontal bar primitive. Methods stack top-to-bottom, longest bar
    at the top (we pre-sort)."""
    y = np.arange(len(methods))
    colors = [GLOBAL_METHOD_COLORS.get(m, "#888888") for m in methods]
    ax.barh(y, values, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_yticks(y)
    ax.set_yticklabels([_short_method(m) for m in methods])
    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.grid(True, axis="x")
    ax.tick_params(axis="y", length=0)


def plot_method_memory(totals_df: pd.DataFrame,
                       out_prefix: str = "fig_5_2_memory",
                       ) -> List[Path]:
    """Per dataset: horizontal bar of peak cache footprint (bytes).
    Source is ``peak_cache_bytes`` — the per-query ``Cache Size (bytes)``
    reading at its maximum across the trace, summed across spans via each
    span's ``calculateDeepMemorySize``."""
    if totals_df is None or totals_df.empty \
            or "peak_cache_bytes" not in totals_df.columns:
        print("  §5.2 memory: empty/incompatible totals; skipping")
        return []
    paths: List[Path] = []
    for dataset in sorted(totals_df["dataset"].unique()):
        sub = totals_df[totals_df["dataset"] == dataset].copy()
        # Drop NaN/negative cache values (sentinel for no-cache methods).
        sub = sub[sub["peak_cache_bytes"].fillna(-1) >= 0]
        if sub.empty:
            continue
        sub = _ordered_method_subframe(sub, sort_key="peak_cache_bytes")
        fig, ax = _new_fig()
        _hbar(ax, sub["method"].tolist(),
              sub["peak_cache_bytes"].to_numpy(),
              xlabel="Peak cache memory (B)")
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: _sci(v)))
        paths.append(_save(fig, f"{out_prefix}_{_safe(dataset)}"))
    return paths


def plot_method_time(totals_df: pd.DataFrame,
                     out_prefix: str = "fig_5_2_time",
                     ) -> List[Path]:
    """Per dataset: horizontal bar of total wall-clock (s)."""
    if totals_df is None or totals_df.empty:
        print("  §5.2 time: empty totals; skipping")
        return []
    paths: List[Path] = []
    for dataset in sorted(totals_df["dataset"].unique()):
        sub = _ordered_method_subframe(
            totals_df[totals_df["dataset"] == dataset].copy(),
            sort_key="total_time")
        fig, ax = _new_fig()
        _hbar(ax, sub["method"].tolist(), sub["total_time"].to_numpy(),
              xlabel="Total wall-clock (s)")
        paths.append(_save(fig, f"{out_prefix}_{_safe(dataset)}"))
    return paths


# Backwards-compat wrapper: emits the memory + time panels.
def plot_method_cost(totals_df: pd.DataFrame,
                     out_prefix: str = "fig_5_2",
                     **_,
                     ) -> List[Path]:
    paths: List[Path] = []
    paths.extend(plot_method_memory(totals_df,
                                    out_prefix=f"{out_prefix}_memory"))
    paths.extend(plot_method_time(totals_df, out_prefix=f"{out_prefix}_time"))
    return paths


# ---------------------------------------------------------------------------
# §5.3 risk-coverage — three half-column charts vs ρ_P.
# ---------------------------------------------------------------------------

def _sci(v: float) -> str:
    """Compact tick formatter — k / M / G suffixes (axes ticks only).
    Falls back to engineering exponent for very large numbers."""
    if v == 0 or not np.isfinite(v):
        return "0"
    av = abs(v)
    if av >= 1e9:
        return f"{v / 1e9:.1f}G"
    if av >= 1e6:
        return f"{v / 1e6:.1f}M"
    if av >= 1e3:
        return f"{v / 1e3:.1f}k"
    if av >= 1:
        return f"{v:.0f}"
    return f"{v:.2f}"


def plot_risk_coverage_f1(summary_df: pd.DataFrame,
                          name: str = "fig_5_3_f1_vs_rho",
                          ) -> Optional[Path]:
    """Precision + recall vs ρ_P (F1 dropped — recall pins at 1.0 under the
    all-matches matcher, so precision carries the trade-off on its own)."""
    if summary_df is None or summary_df.empty \
            or "mean_precision" not in summary_df.columns:
        return None
    sub = summary_df.sort_values("rho_p")
    fig, ax = _new_fig()
    ax.plot(sub["rho_p"], sub["mean_precision"], marker="s",
            color=_C_SECONDARY, label="P")
    ax.plot(sub["rho_p"], sub["mean_recall"], marker="^",
            color=_C_TERTIARY, label="R")
    ax.set_ylim(-0.02, 1.05)
    ax.set_xlabel(r"$\rho_P$")
    ax.set_ylabel("Score")
    ax.legend(loc="lower right", ncol=2, handlelength=1.0,
              columnspacing=0.6, borderpad=0.2)
    return _save(fig, name)


def plot_risk_coverage_bound(summary_df: pd.DataFrame,
                             name: str = "fig_5_3_bound_vs_rho",
                             ) -> Optional[Path]:
    """Per-query bound-pass rate vs ρ_P: fraction of pattern queries whose
    union-precision meets the SLO 1−ρ_P. Coarse denominator (#queries) — the
    smoother match-weighted picture is in plot_risk_coverage_overshoot."""
    if summary_df is None or summary_df.empty:
        return None
    sub = summary_df.sort_values("rho_p")
    fig, ax = _new_fig()
    ax.plot(sub["rho_p"], sub["bound_pass_rate"], marker="o",
            color=_C_WARN)
    ax.axhline(1.0, color="black", linestyle=(0, (2, 2)), linewidth=0.6,
               alpha=0.5)
    ax.set_ylim(-0.02, 1.05)
    ax.set_xlabel(r"$\rho_P$")
    ax.set_ylabel("Bound pass rate")
    return _save(fig, name)


def plot_risk_coverage_overshoot(summary_df: pd.DataFrame,
                                 name: str = "fig_5_3_overshoot_vs_rho",
                                 ) -> Optional[Path]:
    """Match-weighted mean per-match overshoot vs ρ_P. The y=ρ_P reference
    line is the SLO target: points below it mean the bound holds in
    aggregate (mean overshoot ≤ allowed). Denominator is total #matches."""
    if summary_df is None or summary_df.empty or \
            "mean_overshoot" not in summary_df.columns:
        return None
    sub = summary_df.sort_values("rho_p")
    fig, ax = _new_fig()
    ax.plot(sub["rho_p"], sub["mean_overshoot"], marker="o",
            color=_C_WARN, label="mean")
    # y=ρ_P reference — SLO target.
    xs = sub["rho_p"].to_numpy()
    if len(xs) >= 2:
        ref = np.linspace(xs.min(), xs.max(), 50)
        ax.plot(ref, ref, color="black", linestyle=(0, (2, 2)),
                linewidth=0.6, alpha=0.6, label=r"$y=\rho_P$")
    ax.set_xlabel(r"$\rho_P$  (target overshoot)")
    ax.set_ylabel(r"Mean overshoot")
    ax.legend(loc="upper left", handlelength=1.0, columnspacing=0.6,
              borderpad=0.2)
    return _save(fig, name)



def plot_risk_coverage_time(summary_df: pd.DataFrame,
                            name: str = "fig_5_3_time_vs_rho",
                            ) -> Optional[Path]:
    """Total query time and total IO vs ρ_P on a twin-y axis. Looser SLO
    (larger ρ_P) ⇒ fewer refinements ⇒ less work; the two curves should
    fall together as ρ_P grows."""
    if summary_df is None or summary_df.empty:
        return None
    sub = summary_df.sort_values("rho_p")
    fig, ax = _new_fig()
    l1, = ax.plot(sub["rho_p"], sub["total_time"], marker="o",
                  color=_C_PRIMARY, label="time")
    ax.set_xlabel(r"$\rho_P$")
    ax.set_ylabel("Total query time (s)", color=_C_PRIMARY)
    ax.tick_params(axis="y", labelcolor=_C_PRIMARY)
    ax2 = ax.twinx()
    l2, = ax2.plot(sub["rho_p"], sub["total_io"], marker="s",
                   color=_C_SECONDARY, label="IO")
    ax2.set_ylabel("Total IO", color=_C_SECONDARY)
    ax2.tick_params(axis="y", labelcolor=_C_SECONDARY)
    ax.legend([l1, l2], ["time", "IO"], loc="upper right",
              handlelength=1.0, columnspacing=0.6, borderpad=0.2)
    return _save(fig, name)


def plot_risk_coverage(summary_df: pd.DataFrame, **_) -> List[Path]:
    paths: List[Path] = []
    for fn in (plot_risk_coverage_f1,
               plot_risk_coverage_bound,
               plot_risk_coverage_overshoot,
               plot_risk_coverage_time):
        p = fn(summary_df)
        if p is not None:
            paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# §5.4 bound tightness — two half-column charts vs α.
# ---------------------------------------------------------------------------

def plot_bound_width(summary_df: pd.DataFrame,
                     name: str = "fig_5_4_width_vs_alpha",
                     ) -> Optional[Path]:
    if summary_df is None or summary_df.empty:
        return None
    sub = summary_df.dropna(subset=["alpha_sweep"]).sort_values("alpha_sweep")
    fig, ax = _new_fig()
    ax.plot(sub["alpha_sweep"], sub["mean"], marker="o",
            color=_C_WARN)
    ax.set_xscale("log", base=2)
    ax.set_xlabel(r"$\alpha$  (aggregation factor)")
    ax.set_ylabel(r"Mean width (deg)")
    return _save(fig, name)


def plot_bound_unbounded(summary_df: pd.DataFrame,
                         name: str = "fig_5_4_unbounded_vs_alpha",
                         ) -> Optional[Path]:
    if summary_df is None or summary_df.empty:
        return None
    sub = summary_df.dropna(subset=["alpha_sweep"]).sort_values("alpha_sweep")
    fig, ax = _new_fig()
    ax.plot(sub["alpha_sweep"], sub["undefined_share"], marker="o",
            color=_C_WARN)
    ax.set_xscale("log", base=2)
    ax.set_ylim(-0.02, 1.02)
    ax.set_xlabel(r"$\alpha$")
    ax.set_ylabel(r"Unbounded share")
    return _save(fig, name)


def plot_bound_tightness(summary_df: pd.DataFrame, **_) -> List[Path]:
    paths: List[Path] = []
    for fn in (plot_bound_width, plot_bound_unbounded):
        p = fn(summary_df)
        if p is not None:
            paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# §5.5 visual quality — per-dataset SSIM + pixel-diff boxplots.
# Methods on Y axis (horizontal boxplots) so labels never collide at half-col.
# ---------------------------------------------------------------------------

def _hbox(ax: plt.Axes, methods: List[str], data: List[np.ndarray]) -> None:
    bp = ax.boxplot(data, vert=False, widths=0.55, patch_artist=True,
                    showmeans=True, meanline=False,
                    meanprops={"marker": "o", "markersize": 3,
                               "markerfacecolor": "white",
                               "markeredgecolor": "black",
                               "markeredgewidth": 0.6},
                    medianprops={"color": "black", "linewidth": 1.0},
                    whiskerprops={"linewidth": 0.7},
                    capprops={"linewidth": 0.7},
                    flierprops={"marker": ".", "markersize": 2,
                                "markeredgewidth": 0.0,
                                "markerfacecolor": "#555"})
    for patch, m in zip(bp["boxes"], methods):
        patch.set_facecolor(GLOBAL_METHOD_COLORS.get(m, "#888888"))
        patch.set_alpha(0.75)
        patch.set_edgecolor("black")
        patch.set_linewidth(0.6)
    ax.set_yticks(range(1, len(methods) + 1))
    ax.set_yticklabels([_short_method(m) for m in methods])
    ax.invert_yaxis()
    ax.tick_params(axis="y", length=0)


def plot_visual_ssim(visual_df: pd.DataFrame,
                     out_prefix: str = "fig_5_5_ssim",
                     ) -> List[Path]:
    if visual_df is None or visual_df.empty:
        return []
    paths: List[Path] = []
    for dataset in visual_df["dataset"].unique():
        sub = visual_df[visual_df["dataset"] == dataset]
        methods = _method_order(sub)
        data = [sub[sub["method"] == m]["ssim"].dropna().values
                for m in methods]
        fig, ax = _new_fig()
        _hbox(ax, methods, data)
        ax.set_xlabel(r"SSIM  ($\uparrow$ better)")
        ax.grid(True, axis="x")
        paths.append(_save(fig, f"{out_prefix}_{_safe(dataset)}"))
    return paths


def plot_visual_pixdiff(visual_df: pd.DataFrame,
                        out_prefix: str = "fig_5_5_pixdiff",
                        ) -> List[Path]:
    if visual_df is None or visual_df.empty:
        return []
    paths: List[Path] = []
    for dataset in visual_df["dataset"].unique():
        sub = visual_df[visual_df["dataset"] == dataset]
        methods = _method_order(sub)
        data = [sub[sub["method"] == m]["pixel_diff_percentage"].dropna().values
                for m in methods]
        fig, ax = _new_fig()
        _hbox(ax, methods, data)
        ax.set_xlabel(r"Pixel diff (%)  ($\downarrow$ better)")
        ax.grid(True, axis="x")
        paths.append(_save(fig, f"{out_prefix}_{_safe(dataset)}"))
    return paths


def plot_visual_metrics(visual_df: pd.DataFrame, **_) -> List[Path]:
    return plot_visual_ssim(visual_df) + plot_visual_pixdiff(visual_df)


# ---------------------------------------------------------------------------
# §5.6 cohort sweep — one half-column bar chart per axis. F1 bars; if mean_io
# is in the frame we add a thin secondary line.
# ---------------------------------------------------------------------------

_COHORT_BUCKET_ORDER = {
    "length":      ["2seg", "4seg", "8seg"],
    "width":       ["narrow", "medium", "wide"],
    "selectivity": ["many", "med", "few"],
}

_AXIS_LABEL = {
    "length":      "Pattern length",
    "width":       "Angle band",
    "selectivity": "Match count",
}


def _plot_cohort_f1(summary_df: pd.DataFrame, axis_name: str,
                    f1_col: str, ylabel: str, name: str) -> Optional[Path]:
    """Cohort F1 bar chart for one axis. ``f1_col`` selects macro_f1 or
    micro_f1; the plot is otherwise identical so the macro/micro pair stays
    visually comparable."""
    if summary_df is None or summary_df.empty:
        return None
    if axis_name not in summary_df["cohort_axis"].unique():
        return None
    if f1_col not in summary_df.columns:
        return None
    sub = summary_df[summary_df["cohort_axis"] == axis_name].copy()
    order = _COHORT_BUCKET_ORDER[axis_name]
    sub["_order"] = sub["cohort_bucket"].apply(
        lambda b: order.index(b) if b in order else 99)
    sub = sub.sort_values("_order")
    buckets = sub["cohort_bucket"].tolist()
    f1 = sub[f1_col].tolist()
    xs = np.arange(len(buckets))

    fig, ax = _new_fig()
    sk_color = GLOBAL_METHOD_COLORS.get("VASTA", _C_PRIMARY)
    ax.bar(xs, f1, color=sk_color, alpha=0.85, edgecolor="black",
           linewidth=0.5, width=0.65)
    for x, v in zip(xs, f1):
        ax.text(x, v + 0.02, f"{v:.2f}", ha="center", va="bottom",
                fontsize=6)
    ax.set_xticks(xs)
    ax.set_xticklabels(buckets)
    ax.set_xlabel(_AXIS_LABEL.get(axis_name, axis_name))
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, 1.15)
    ax.grid(True, axis="y")
    return _save(fig, name)


def plot_cohort_axis_macro_f1(summary_df: pd.DataFrame, axis_name: str
                              ) -> Optional[Path]:
    """Macro-F1 (mean of per-query F1) for the cohort axis."""
    col = "macro_f1" if "macro_f1" in summary_df.columns else "mean_f1"
    return _plot_cohort_f1(summary_df, axis_name, col, "Macro F1",
                           f"fig_5_6_{axis_name}_macroF1")


def plot_cohort_axis_micro_f1(summary_df: pd.DataFrame, axis_name: str
                              ) -> Optional[Path]:
    """Micro-F1 (F1 over aggregated TP/FP/FN) for the cohort axis."""
    return _plot_cohort_f1(summary_df, axis_name, "micro_f1", "Micro F1",
                           f"fig_5_6_{axis_name}_microF1")


def plot_cohort_axis_precision(summary_df: pd.DataFrame, axis_name: str
                               ) -> Optional[Path]:
    """Macro precision (mean of per-query union precision) for the cohort
    axis. Used in the paper since recall is pinned at ~1.0 under the
    all-matches matcher, so precision carries the cohort-level story."""
    col = ("mean_union_precision" if "mean_union_precision" in summary_df.columns
           else "mean_precision")
    return _plot_cohort_f1(summary_df, axis_name, col, "Precision",
                           f"fig_5_6_{axis_name}_precision")


# Backwards-compat alias: the old plot_cohort_axis used mean_f1 (macro).
def plot_cohort_axis(summary_df: pd.DataFrame, axis_name: str,
                     name: Optional[str] = None) -> Optional[Path]:
    if name is None:
        return plot_cohort_axis_macro_f1(summary_df, axis_name)
    col = "macro_f1" if "macro_f1" in summary_df.columns else "mean_f1"
    return _plot_cohort_f1(summary_df, axis_name, col, "Macro F1", name)


def plot_cohort_summary(summary_df: pd.DataFrame, **_) -> List[Path]:
    paths: List[Path] = []
    for axis_name in _COHORT_BUCKET_ORDER:
        for fn in (plot_cohort_axis_precision,
                   plot_cohort_axis_macro_f1,
                   plot_cohort_axis_micro_f1):
            p = fn(summary_df, axis_name)
            if p is not None:
                paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# §5.8 scalability — cumulative time vs row count, one line per method.
# ---------------------------------------------------------------------------

_SCALABILITY_MARKERS = {
    "OLS-0":      "o",
    "OLS-C":      "s",
    "VASTA": "^",
}


def plot_scalability(scalability_df: pd.DataFrame,
                     name: str = "fig_5_8_scalability",
                     ) -> Optional[Path]:
    """Log-log cumulative query time vs time-series length. One line per
    method; methods with fewer than 2 measured datasets are still drawn (a
    single marker is informative when the larger scales DNF'd)."""
    if scalability_df is None or scalability_df.empty:
        return None
    fig, ax = _new_fig()
    for method in _method_order(scalability_df):
        sub = (scalability_df[scalability_df["method"] == method]
                 .sort_values("n_rows"))
        if sub.empty:
            continue
        ax.plot(sub["n_rows"], sub["total_time"],
                marker=_SCALABILITY_MARKERS.get(method, "o"),
                color=GLOBAL_METHOD_COLORS.get(method, "#888888"),
                label=_short_method(method))
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Rows")
    ax.set_ylabel("Cumulative time (s)")
    ax.legend(loc="upper left", ncol=1, handlelength=1.0,
              columnspacing=0.6, borderpad=0.2)
    return _save(fig, name)