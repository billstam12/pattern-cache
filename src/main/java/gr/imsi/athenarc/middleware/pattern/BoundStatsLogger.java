package gr.imsi.athenarc.middleware.pattern;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Per-sketch slope-bound logger for the D1/D2 bound-tightness experiment.
 *
 * <p>Disabled by default. The harness enables it once per run via
 * {@link #configure(String, int)}, then the pattern executor calls
 * {@link #dumpSketches(int, List, AggregateInterval)} after the initial sketch
 * population to record one row per (queryIndex, aggFactor, sketch). Rerunning
 * the workload at different {@code -agg} values across runs lets analysis
 * aggregate per-α statistics (mean interval width, fraction of undefined / Q⁻
 * ≤ 0 sketches) — D1 and D2 of §5.
 *
 * <p>Output schema (CSV, one file per run, appended across queries):
 * <pre>
 *   query_index, agg_factor, sketch_idx, sketch_from_ms, sketch_to_ms,
 *   has_initialized, min_angle_deg, max_angle_deg, width_deg, undefined
 * </pre>
 *
 * <p>{@code undefined} captures the Sketch's "no sound slope bound" state
 * (Q⁻ ≤ 0 or fewer than two sub-buckets), i.e. the paper's unbounded regime.
 */
public final class BoundStatsLogger {

    private static final Logger LOG = LoggerFactory.getLogger(BoundStatsLogger.class);

    private static volatile boolean enabled = false;
    private static volatile Path outFile;
    private static volatile int currentQueryIndex = -1;

    private BoundStatsLogger() {}

    /**
     * Enable logging for the given run. Creates {@code <outDir>/bound_stats/run_<n>.csv}
     * (with header) under the experiment's output folder. Safe to call once per
     * run; subsequent dumpSketches calls append.
     */
    public static synchronized void configure(String outDir, int runNumber) {
        try {
            Path dir = Paths.get(outDir, "bound_stats");
            Files.createDirectories(dir);
            outFile = dir.resolve("run_" + runNumber + ".csv");
            if (!Files.exists(outFile)) {
                try (BufferedWriter w = Files.newBufferedWriter(outFile)) {
                    w.write("query_index,agg_factor,sketch_idx,sketch_from_ms,sketch_to_ms,"
                            + "has_initialized,min_angle_deg,max_angle_deg,width_deg,undefined");
                    w.newLine();
                }
            }
            enabled = true;
            LOG.info("BoundStatsLogger enabled → {}", outFile);
        } catch (IOException e) {
            LOG.warn("BoundStatsLogger configure failed: {}", e.getMessage());
            enabled = false;
        }
    }

    public static synchronized void disable() {
        enabled = false;
        outFile = null;
        currentQueryIndex = -1;
    }

    public static void setCurrentQuery(int queryIndex) {
        currentQueryIndex = queryIndex;
    }

    public static boolean isEnabled() {
        return enabled && outFile != null;
    }

    /**
     * Append one row per sketch in {@code sketches} at {@code aggFactor}. A
     * Sketch whose angle bound is non-finite (the paper's Q⁻ ≤ 0 regime) is
     * recorded with {@code undefined=true} and {@code width_deg=NaN}; an
     * uninitialised one (data-missing) with {@code has_initialized=false}.
     */
    public static synchronized void dumpSketches(int aggFactor,
                                                 List<Sketch> sketches,
                                                 AggregateInterval timeUnit) {
        if (!isEnabled() || sketches == null) return;
        try (BufferedWriter w = Files.newBufferedWriter(outFile, StandardOpenOption.APPEND)) {
            for (int i = 0; i < sketches.size(); i++) {
                Sketch s = sketches.get(i);
                long from = s.getFrom();
                long to = s.getTo();
                boolean init = s.hasInitialized();
                double lo = s.getMinAngle();
                double hi = s.getMaxAngle();
                boolean undef = !init
                        || !Double.isFinite(lo) || !Double.isFinite(hi);
                double width = undef ? Double.NaN : (hi - lo);
                w.write(currentQueryIndex + "," + aggFactor + "," + i + ","
                        + from + "," + to + ","
                        + init + ","
                        + (Double.isFinite(lo) ? lo : "")
                        + ","
                        + (Double.isFinite(hi) ? hi : "")
                        + ","
                        + (Double.isFinite(width) ? width : "")
                        + ","
                        + undef);
                w.newLine();
            }
        } catch (IOException e) {
            LOG.warn("BoundStatsLogger dump failed (query {}, aggFactor {}): {}",
                    currentQueryIndex, aggFactor, e.getMessage());
        }
    }
}
