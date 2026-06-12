package gr.imsi.athenarc.experiments;

import gr.imsi.athenarc.middleware.sketch.OLSSlopeBounds;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Bound-validation experiment.
 *
 * Slides a fixed-length window across a DuckDB-loaded time series and, at each
 * position, compares:
 *  - the closed-form OLS slope bound from {@link OLSSlopeBounds} computed over
 *    sub-bucket min/max envelopes (what the cache reports), against
 *  - the ground-truth OLS slope of the underlying raw points (commons-math3
 *    SimpleRegression).
 *
 * Reports per-window slack and containment, plus an aggregate summary.
 *
 * Usage (defaults shown):
 *   java -cp target/pattern-cache-1.0-SNAPSHOT.jar \
 *        gr.imsi.athenarc.experiments.BoundValidation \
 *        --duckdb /tmp/vasta.duckdb \
 *        --table  synthetic_patterns \
 *        --measure synthetic_pat \
 *        --from   2024-01-01T00:00:00Z \
 *        --to     2024-01-01T02:00:00Z \
 *        --segMs  300000 \
 *        --stepMs 60000 \
 *        --subBucketMs 15000 \
 *        --out output/synth_10y_1m/bound_validation.csv
 */
public final class BoundValidation {

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);
        System.out.println("=== Bound validation ===");
        System.out.println("  duckdb=" + a.duckdb);
        System.out.println("  table=" + a.table + "  measure=" + a.measure);
        System.out.printf("  range=[%d, %d) (%d ms span)%n", a.fromMs, a.toMs, a.toMs - a.fromMs);
        System.out.printf("  segMs=%d stepMs=%d subBucketMs=%d  (sub-buckets per segment=%d)%n",
                a.segMs, a.stepMs, a.subBucketMs, a.segMs / a.subBucketMs);

        Path outPath = Paths.get(a.outCsv);
        Path parent = outPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        int total = 0, contains = 0, undefined = 0;
        double sumSlackDeg = 0;
        double maxSlackDeg = 0;
        double sumGapToBoundDeg = 0;          // distance from true slope to nearest bound edge when outside
        int outside = 0;

        try (Connection c = DriverManager.getConnection("jdbc:duckdb:" + a.duckdb);
             BufferedWriter out = Files.newBufferedWriter(outPath)) {

            out.write("seg_from_ms,seg_from_iso,n_raw,n_subbuckets,"
                    + "true_slope_per_min,true_angle_deg,"
                    + "bound_lower_slope_per_min,bound_upper_slope_per_min,"
                    + "min_angle_deg,max_angle_deg,slack_deg,contains\n");

            for (long segFrom = a.fromMs; segFrom + a.segMs <= a.toMs; segFrom += a.stepMs) {
                long segTo = segFrom + a.segMs;
                Segment s = computeSegment(c, a.table, a.measure, segFrom, segTo, a.subBucketMs);
                total++;
                if (!s.boundDefined) {
                    undefined++;
                    out.write(String.format(Locale.ROOT,
                            "%d,%s,%d,%d,%.6f,%.4f,,,,,,\n",
                            segFrom, isoUtc(segFrom), s.nRaw, s.nSubBuckets,
                            s.trueSlopePerMin, s.trueAngleDeg));
                    continue;
                }
                double slackDeg = s.maxAngleDeg - s.minAngleDeg;
                boolean cont = s.trueAngleDeg >= s.minAngleDeg - 1e-6
                            && s.trueAngleDeg <= s.maxAngleDeg + 1e-6;
                if (cont) {
                    contains++;
                } else {
                    outside++;
                    double gap = s.trueAngleDeg < s.minAngleDeg
                            ? (s.minAngleDeg - s.trueAngleDeg)
                            : (s.trueAngleDeg - s.maxAngleDeg);
                    sumGapToBoundDeg += gap;
                }
                sumSlackDeg += slackDeg;
                if (slackDeg > maxSlackDeg) maxSlackDeg = slackDeg;
                out.write(String.format(Locale.ROOT,
                        "%d,%s,%d,%d,%.6f,%.4f,%.6f,%.6f,%.4f,%.4f,%.4f,%s\n",
                        segFrom, isoUtc(segFrom), s.nRaw, s.nSubBuckets,
                        s.trueSlopePerMin, s.trueAngleDeg,
                        s.boundLowerSlope, s.boundUpperSlope,
                        s.minAngleDeg, s.maxAngleDeg, slackDeg, cont));
            }
        }

        int defined = total - undefined;
        System.out.println();
        System.out.println("=== Summary ===");
        System.out.printf("  segments total:               %d%n", total);
        System.out.printf("  segments with defined bound:  %d%n", defined);
        System.out.printf("  segments with undefined bound: %d%n", undefined);
        if (defined > 0) {
            System.out.printf("  containment rate:             %d/%d  (%.1f%%)%n",
                    contains, defined, 100.0 * contains / defined);
            System.out.printf("  outside-bound segments:       %d%n", outside);
            if (outside > 0) {
                System.out.printf("  mean gap (true → bound edge): %.4f deg%n",
                        sumGapToBoundDeg / outside);
            }
            System.out.printf("  mean slack:                   %.4f deg%n", sumSlackDeg / defined);
            System.out.printf("  max slack:                    %.4f deg%n", maxSlackDeg);
        }
        System.out.println("  csv:                          " + outPath.toAbsolutePath());
    }

    private static Segment computeSegment(Connection c, String table, String measure,
                                          long fromMs, long toMs, long subBucketMs) throws Exception {
        Segment s = new Segment();

        // -- Ground truth: OLS over raw points, x in minutes from segment start.
        SimpleRegression reg = new SimpleRegression(true);
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT epoch_ms(timestamp) AS ts, value FROM " + table
                             + " WHERE id = '" + measure + "'"
                             + " AND timestamp >= make_timestamp(CAST(" + (fromMs * 1000L) + " AS BIGINT))"
                             + " AND timestamp <  make_timestamp(CAST(" + (toMs   * 1000L) + " AS BIGINT))"
                             + " ORDER BY timestamp")) {
            int n = 0;
            while (rs.next()) {
                long ts = rs.getLong("ts");
                double v = rs.getDouble("value");
                double xMin = (ts - fromMs) / 60_000.0;
                reg.addData(xMin, v);
                n++;
            }
            s.nRaw = n;
        }
        s.trueSlopePerMin = reg.getSlope();
        s.trueAngleDeg = Math.toDegrees(Math.atan(s.trueSlopePerMin));

        // -- Bound: sub-bucket min/max envelope, x at sub-bucket midpoints in minutes.
        List<double[]> rows = new ArrayList<>(); // {x_mid_minutes, yMin, yMax}
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT FLOOR((epoch_ms(timestamp) - " + fromMs + ") / " + subBucketMs + ") AS sb,"
                             + " min(value) AS mn, max(value) AS mx, count(*) AS cnt"
                             + " FROM " + table
                             + " WHERE id = '" + measure + "'"
                             + " AND timestamp >= make_timestamp(CAST(" + (fromMs * 1000L) + " AS BIGINT))"
                             + " AND timestamp <  make_timestamp(CAST(" + (toMs   * 1000L) + " AS BIGINT))"
                             + " GROUP BY sb ORDER BY sb")) {
            while (rs.next()) {
                long sb = rs.getLong("sb");
                double mn = rs.getDouble("mn");
                double mx = rs.getDouble("mx");
                double xMidMs = sb * subBucketMs + subBucketMs / 2.0;
                double xMidMin = xMidMs / 60_000.0;
                rows.add(new double[]{xMidMin, mn, mx});
            }
        }
        s.nSubBuckets = rows.size();
        if (rows.size() < 2) {
            s.boundDefined = false;
            return s;
        }
        double[] x = new double[rows.size()];
        double[] yMin = new double[rows.size()];
        double[] yMax = new double[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            x[i] = rows.get(i)[0];
            yMin[i] = rows.get(i)[1];
            yMax[i] = rows.get(i)[2];
        }
        OLSSlopeBounds.Result bound = OLSSlopeBounds.compute(x, yMin, yMax);
        s.boundDefined = bound.defined;
        if (bound.defined) {
            s.boundLowerSlope = bound.slopeLower;
            s.boundUpperSlope = bound.slopeUpper;
            s.minAngleDeg = Math.toDegrees(Math.atan(bound.slopeLower));
            s.maxAngleDeg = Math.toDegrees(Math.atan(bound.slopeUpper));
        }
        return s;
    }

    private static String isoUtc(long ms) {
        return java.time.Instant.ofEpochMilli(ms).toString();
    }

    private static final class Segment {
        int nRaw;
        int nSubBuckets;
        double trueSlopePerMin;
        double trueAngleDeg;
        boolean boundDefined;
        double boundLowerSlope;
        double boundUpperSlope;
        double minAngleDeg;
        double maxAngleDeg;
    }

    private static final class Args {
        String duckdb = "/tmp/vasta.duckdb";
        String table = "synthetic_patterns";
        String measure = "synthetic_pat";
        long fromMs = 1704067200000L;     // 2024-01-01T00:00:00Z
        long toMs   = 1704074400000L;     // 2024-01-01T02:00:00Z
        long segMs  = 300_000L;            // 5 min
        long stepMs = 60_000L;             // 1 min
        long subBucketMs = 15_000L;        // 15 s
        String outCsv = "output/synth_10y_1m/bound_validation.csv";

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length - 1; i += 2) {
                String k = argv[i];
                String v = argv[i + 1];
                switch (k) {
                    case "--duckdb":      a.duckdb = v; break;
                    case "--table":       a.table = v; break;
                    case "--measure":     a.measure = v; break;
                    case "--from":        a.fromMs = java.time.Instant.parse(v).toEpochMilli(); break;
                    case "--to":          a.toMs   = java.time.Instant.parse(v).toEpochMilli(); break;
                    case "--segMs":       a.segMs = Long.parseLong(v); break;
                    case "--stepMs":      a.stepMs = Long.parseLong(v); break;
                    case "--subBucketMs": a.subBucketMs = Long.parseLong(v); break;
                    case "--out":         a.outCsv = v; break;
                    default: throw new IllegalArgumentException("Unknown arg: " + k);
                }
            }
            return a;
        }
    }
}
