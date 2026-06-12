# VASTA — Visualization And Search over Time Series Aggregates

This repository contains the code, experiment harness, and analysis used to
reproduce the results and figures in our paper *"VASTA: Visualization And
Search over Time Series Aggregates"*.

**VASTA** is a single shared cache that serves both visual (view) operations and
pattern-search queries from one set of `(min, max, count, sum)` aggregates, with
on-demand refinement driven by a per-query tolerance `ρ`. In the code, VASTA is
selected with the algorithm id `approxOls` (the Java `-method` argument); it is
labelled **VASTA** in every output folder, metric table, and figure.

The evaluation answers five questions, each mapped to one experiment and one
figure group:

| Paper § | Question | Experiment (`experiments.sh`) | Figures |
|---------|----------|-------------------------------|---------|
| §5.2 | **Q1** — cost of one shared cache vs. siloed / no-cache alternatives | `methods` (`NoC=1`) + `siloed` (`MR=1`) | `fig_5_2_time_*`, `fig_5_2_memory_*` |
| §5.3 | **Q2** — how tightly `ρ_P` steers what is fetched and returned | `risk` (run with `MATCH_SEL=all`) | `fig_5_3_{f1,time,overshoot,bound}_vs_rho` |
| §5.4 | **Q3** — slope-interval tightness vs. aggregation factor `α` | `bound` | `fig_5_4_{width,unbounded}_vs_alpha_*` |
| §5.5 | **Q4** — visual fidelity vs. the exact render | `methods` (`NoC=1`) | `fig_5_5_{ssim,pixdiff}_*` |
| §5.6 | **Q4** — accuracy across length / angle-width / selectivity cohorts | `cohorts` | `fig_5_6_{length,width,selectivity}_precision` |
| §5.8 | **Q5** — how cumulative cost scales with series length | `methods` on the SNT family | `fig_5_8_scalability` |

The methods compared throughout are:

| Label in figures | Code label | Description |
|------------------|-----------|-------------|
| **VASTA** | `approxOls` | Our proposed shared cache — `(min,max,count,sum)`, scoped refinement |
| **OLS-C** | `ols` (`timeCacheQueries`) | Exact OLS aggregate cache (seven-tuple regression sums) |
| **OLS-0** | `ols` (`timeAggregateQueries`) | No-cache exact OLS baseline (the visual / pattern ground truth) |
| **Silo-0** | `minmax` + `OLS-0` | Siloed composite: MinMaxCache for views, OLS-0 for patterns |
| **Silo-MR** | `minmax` + `MATCH_RECOGNIZE` | Siloed composite with SQL:2016 `MATCH_RECOGNIZE` (Trino) for patterns |

Datasets: **MNF** (`manufacturing`, 20M points), **SOCC** (`soccer`, 350M
points), and the synthetic **SNT5M–SNT1B** family (`synth_10y_1m`,
`synth_10y_5s`, `synth_10y_1s`, `synth_10y_316ms`).

---

## 1. Prerequisites

- **Java 11+ and Maven** — to build and run the pattern-cache application.
- **A database backend.** By **default** the harness connects to the shared lab
  server (`USE_REMOTE=1`), where the datasets are already loaded. Access uses a
  **read-only guest account** that is enough to run every experiment (they only
  read) — to use this path, please get in touch (`bstam@athenarc.gr`) and we'll
  send you the `application.properties` file with the server connection details.
  - **Docker** (`USE_REMOTE=0`) — otherwise, run the experiments locally: the
    `methods` / `siloed` / `bound` / `risk` / `cohorts` subcommands auto-start a
    local TimescaleDB (or Postgres) container via `scripts/timescale/start.sh`.
    This path loads the data itself, so it needs the raw CSVs below.
- **Trino v.476+** — only for the `Silo-MR` composite (`MR=1`), which needs
  `MATCH_RECOGNIZE` (TimescaleDB/Postgres do not implement it). It too defaults
  to the remote server, **which may not be reachable** — if you want to run the
  `Silo-MR` configuration, please get in touch first (`bstam@athenarc.gr`).
- **Raw data CSVs** — only needed for the local Docker path (`USE_REMOTE=0`),
  which loads the data itself; the default remote server already has them. Place
  them under `${DATA_DIR}` (default `/opt/exp-data`), named `<dataset>.csv`. The
  real datasets (MNF, SOCC) are available here:
  <https://drive.google.com/open?id=1u6ahXz_LABmGQmLJxOAgd5QSbg5FB-ve&usp=drive_fs>;
  the synthetic family is produced by
  `scripts/data/generate_synthetic_datasets.sh`.
- **Python 3** — for the analysis + figure step. Install the dependencies:
  ```bash
  pip install -r requirements.txt
  ```

---

## 2. Running the experiments

All experiments go through one dispatcher, `scripts/experiments.sh`, which
builds per-method output folders under
`output/<dataset>/<db>[/matchall]/[<variant>/]<MethodLabel>/run_<N>/`. The
`analysis` subcommand then reads those folders back and renders the figures into
`figures/<dataset>/`.

Every experiment is run **5 times** (`RUNS=5`, the default). Reported query time
and fetched-row counts are averaged (mean) over the 5 runs, and peak cache
footprint is the max across them. Accuracy metrics (precision/recall/F1, SSIM,
slope-bound width) do not vary run-to-run, so they are computed per query/segment
and averaged over the workload.

### 2.1 Build the application

```bash
mvn clean package
```

### 2.2 (Optional) regenerate the query workloads

The query files under `queries/<dataset>/` are committed, so this is **not
required**. To regenerate them (interleaved Markov-walk trace, vis/pattern
split, `ρ_P` variants, and §5.6 cohort files) for a dataset:

```bash
scripts/queries/generate_all.sh manufacturing
```

### 2.3 Run the experiment subcommands

`experiments.sh <subcommand>` is parameterised by environment variables —
the most important are `TABLE` (dataset/workload) and `DB` (backend):

```bash
TABLE=synth_10y_1m   # default; one of manufacturing | soccer | synth_10y_*
DB=timescale         # timescale | postgres | trino
RUNS=5               # runs per method
AGG=4                # initial aggregation factor α for VASTA
USE_REMOTE=1         # default: shared lab server; set USE_REMOTE=0 for local Docker
```

> The recipes below use the default `USE_REMOTE=1`, running against the shared
> lab server with the read-only guest account (contact `bstam@athenarc.gr` for
> the `application.properties` file). Prefix a call with `USE_REMOTE=0` to bring
> up a local Docker container instead.

| Subcommand | Produces | Notes |
|------------|----------|-------|
| `methods` | OLS-C + VASTA (and OLS-0 when `NoC=1`) | `NoC=1` is needed for §5.2 cost and the §5.5 visual ground truth |
| `siloed` | Silo-0 (and Silo-MR when `MR=1`) | `MR=1` **requires `DB=trino`** |
| `bound` | α-sweep with `--logBoundStats` | §5.4; override the sweep with `ALPHAS="1 2 4 8 16 32"` |
| `risk` | `ρ_P`-sweep (rewrites the accuracy column) | §5.3; override with `ACCS="0.99 0.95 0.9 0.8 0.5"` |
| `cohorts` | per-cohort VASTA + OLS ground truth | §5.6; needs `queries/<table>/cohorts/` (from `generate_all.sh`) |
| `ablation` | VASTA with `calendarAlignment=false` | the no-calendar-alignment ablation |
| `analysis` | every figure + `exp_*.csv` table | Python-only, no DB |
| `all` | `methods` (`NoC=1`) + `siloed` + `bound` + `risk` + `ablation` + `analysis` | full pipeline for one dataset |

**Reproducing each paper figure group end-to-end.** Run the experiment(s) for
the group, then `analysis` with the same `TABLE`/`DB`/`MATCH_SEL` to render the
figures into `figures/<dataset>/`:

```bash
# §5.2 cost + §5.5 visual — MNF and SOCC (NoC=1 adds the OLS-0 ground truth)
NoC=1 TABLE=manufacturing DB=timescale sh scripts/experiments.sh methods
NoC=1 TABLE=soccer        DB=timescale sh scripts/experiments.sh methods
# add the siloed composites for the §5.2 comparison
TABLE=manufacturing DB=timescale sh scripts/experiments.sh siloed
DB=trino MR=1 TABLE=manufacturing sh scripts/experiments.sh siloed   # Silo-MR
TABLE=manufacturing DB=timescale sh scripts/experiments.sh analysis
TABLE=soccer        DB=timescale sh scripts/experiments.sh analysis

# §5.3 tolerance — MNF, all-matches matcher (paper numbers come from matchall)
MATCH_SEL=all TABLE=manufacturing DB=timescale sh scripts/experiments.sh risk
MATCH_SEL=all TABLE=manufacturing DB=timescale sh scripts/experiments.sh analysis

# §5.4 bound tightness — SOCC α-sweep
TABLE=soccer DB=timescale sh scripts/experiments.sh bound
TABLE=soccer DB=timescale sh scripts/experiments.sh analysis

# §5.6 cohorts — MNF length / angle-width / selectivity
TABLE=manufacturing DB=timescale sh scripts/experiments.sh cohorts
TABLE=manufacturing DB=timescale sh scripts/experiments.sh analysis

# §5.8 scalability — run the interleaved trace on each synthetic scale, then plot
for T in synth_10y_1m synth_10y_5s synth_10y_1s synth_10y_316ms; do
  NoC=1 TABLE=$T DB=timescale sh scripts/experiments.sh methods
done
python analysis/analysis_metrics.py --plot --dataset synth_10y_1m --table synth_10y_1m --db timescale \
    --scalability_datasets synth_10y_1m,synth_10y_5s,synth_10y_316ms,synth_10y_1s
```

> `MATCH_SEL=all` nests output under `<db>/matchall/` and forwards
> `--matchSelection all`, so the all-matches recall-attribution runs sit
> alongside the canonical (longest + non-overlap) runs without overwriting them.
> Pass the same `--matcher all` to the analysis to read them back.

### 2.4 Assembling the paper figures

Each `analysis` run writes PDF + PNG into `figures/<dataset>/`. To collect the
exact set the paper uses into `paper_figures/` (created on demand):

```bash
scripts/gather_paper_figures.sh
```

This copies every file listed in `scripts/paper_figures.list` (the curated
source list — edit it to swap a figure between a dataset or matcher variant
without touching the `.tex`). The figure-file → paper mapping:

| Figure file | Paper § / caption |
|-------------|-------------------|
| `fig_5_2_time_{manufacturing,soccer}` | §5.2 — total query time per method (MNF, SOCC) |
| `fig_5_2_memory_{manufacturing,soccer}` | §5.2 — peak cache footprint per method |
| `fig_5_3_f1_vs_rho` | §5.3 — precision & recall vs. `ρ_P` (MNF) |
| `fig_5_3_time_vs_rho` | §5.3 — total query time & fetched rows vs. `ρ_P` |
| `fig_5_3_overshoot_vs_rho` | §5.3 — mean overshoot vs. `ρ_P` |
| `fig_5_3_bound_vs_rho` | §5.3 — per-query bound pass rate vs. `ρ_P` |
| `fig_5_4_width_vs_alpha_soccer` | §5.4 — mean slope-interval width vs. `α` (SOCC) |
| `fig_5_4_unbounded_vs_alpha_soccer` | §5.4 — share of unbounded segments vs. `α` |
| `fig_5_5_ssim_manufacturing` | §5.5 — per-query SSIM vs. the no-cache render |
| `fig_5_5_pixdiff_manufacturing` | §5.5 — per-query pixel-difference % |
| `fig_5_6_{length,width,selectivity}_precision` | §5.6 — precision by cohort axis (MNF) |
| `fig_5_8_scalability` | §5.8 — total query time vs. series length (SNT) |

> The method label shown in the figures is set in
> `analysis/analysis_plots.py` (`_SHORT_METHOD` / `GLOBAL_METHOD_COLORS`).

### 2.5 Running a single method directly

`experiments.sh` calls `scripts/run_method.sh` under the hood; you can invoke it
for a single configuration against the same backend:

```bash
sh scripts/run_method.sh --type timescale --table synth_10y_1m \
  --method approxOls --mode timeCacheQueries \
  --queries queries/synth_10y_1m/queries.txt \
  --out output/synth_10y_1m/timescale/VASTA --runs 1 \
  --adaptation --agg 4 --refinementScope scoped
```

> Results and figures can be found at:  <[https://drive.google.com/open?id=1OPP89PfnndQRsmUTkOkf-nJ094JbI7oc&usp=drive_fs](https://drive.google.com/drive/folders/1yiokSOsn365vRMu51rAmpk9YNtcZFSj6?usp=sharing)>
---

## 3. Repository layout

| Path | Contents |
|------|----------|
| `src/main/java/...` | The Java pattern-cache application (VASTA, OLS baselines, matchers) |
| `scripts/experiments.sh` | Single dispatcher for all experiments |
| `scripts/run_method.sh` | Per-method runner invoked by the dispatcher |
| `scripts/queries/` | Query-workload generators (`generate_all.sh`, splitters) |
| `scripts/data/` | Synthetic-data generators + offline pattern detection |
| `scripts/gather_paper_figures.sh` | Copy curated figures into `paper_figures/` |
| `scripts/paper_figures.list` | The curated figure source list |
| `analysis/analysis_metrics.py` | Entry point: collect metrics, write CSVs + figures |
| `analysis/analysis_utils.py` | Output-folder layout, method specs, collectors |
| `analysis/analysis_plots.py` | All figure rendering (and the VASTA label map) |
| `queries/<dataset>/` | Committed query workloads per dataset |
| `output/<dataset>/<db>/` | Raw per-method output (generated by §2.3; not committed) |
| `figures/<dataset>/` | Generated figures + `exp_*.csv` tables (generated by `analysis`) |
| `paper_figures/` | Curated paper figures (assembled by §2.4; not committed) |

---

## 4. Troubleshooting

1. **Plot rendering** — install the Python deps from `requirements.txt`
   (`scikit-image` for SSIM, `pycairo`/`pillow` for the pixel-difference render).
2. **`MR=1` / Trino errors** — `Silo-MR` needs `DB=trino` (Postgres/Timescale
   don't implement `MATCH_RECOGNIZE`), and the Trino server may not be
   reachable. Contact `bstam@athenarc.gr` if you want to run it.
3. **`cohorts` says the cohort dir is missing** — run
   `scripts/queries/generate_all.sh <dataset>` first.
4. **Java build issues** — ensure Maven and a JDK (11+) are installed.
5. **The run fails to reach the server** — the default (`USE_REMOTE=1`) talks to
   the shared lab server, and there is no automatic fallback. If the server is
   down, re-run with `USE_REMOTE=0` to bring up a local Docker container (this
   path loads the data itself, so it needs the raw CSVs), or contact
   `bstam@athenarc.gr`.
