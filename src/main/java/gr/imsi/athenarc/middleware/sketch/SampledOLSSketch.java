package gr.imsi.athenarc.middleware.sketch;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;

/**
 * A sketch that estimates the OLS slope of a segment from added raw points and
 * exposes a confidence interval on it as an angle bound.
 *
 * <p>{@link #getAngle()} is {@code atan(b)} of the estimated slope. The bound
 * {@link #getMinAngle()}/{@link #getMaxAngle()} is either a closed-form
 * normal-theory interval {@code atan(b +/- z*se)} ({@link CIMethod#CLOSED_FORM}) or
 * an empirical percentile interval from a paired bootstrap over the retained points
 * ({@link CIMethod#BOOTSTRAP}). Points are placed at
 * {@code x = (timestamp - xAnchor) / millisPerXUnit}.
 */
public class SampledOLSSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(SampledOLSSketch.class);

    public static final double DEFAULT_CRITICAL_VALUE = 1.96;
    public static final int DEFAULT_BOOTSTRAP_REPLICATES = 500;
    public static final double DEFAULT_CI_LEVEL = 0.95;

    public enum CIMethod { CLOSED_FORM, BOOTSTRAP }

    private long from;
    private long to;
    private final long windowId;

    private final double millisPerXUnit;
    private final long xAnchor;

    private final CIMethod ciMethod;
    private final double criticalValue;
    private final int bootstrapReplicates;
    private final double ciLevel;
    private final long bootstrapSeed;

    private long   n;
    private double sumX;
    private double sumY;
    private double sumXY;
    private double sumX2;
    private double sumY2;

    private double[] xs;
    private double[] ys;

    private boolean hasInitialized = false;

    private AggregateInterval originalAggregateInterval;

    private boolean computed = false;
    private double angle;
    private double minAngle;
    private double maxAngle;
    private double slope;
    private double slopeStdError;

    public SampledOLSSketch(long from, long to, long windowId) {
        this(from, to, windowId, 1.0, from, DEFAULT_CRITICAL_VALUE);
    }

    public SampledOLSSketch(long from, long to, long windowId,
                            double millisPerXUnit, long xAnchor, double criticalValue) {
        this(from, to, windowId, millisPerXUnit, xAnchor,
             CIMethod.CLOSED_FORM, criticalValue, 0, DEFAULT_CI_LEVEL, 0L);
    }

    public SampledOLSSketch(long from, long to, long windowId,
                            double millisPerXUnit, long xAnchor,
                            int bootstrapReplicates, double ciLevel, long bootstrapSeed) {
        this(from, to, windowId, millisPerXUnit, xAnchor,
             CIMethod.BOOTSTRAP, DEFAULT_CRITICAL_VALUE, bootstrapReplicates, ciLevel, bootstrapSeed);
    }

    private SampledOLSSketch(long from, long to, long windowId,
                             double millisPerXUnit, long xAnchor,
                             CIMethod ciMethod, double criticalValue,
                             int bootstrapReplicates, double ciLevel, long bootstrapSeed) {
        if (!(millisPerXUnit > 0)) {
            throw new IllegalArgumentException("millisPerXUnit must be > 0");
        }
        if (ciMethod == CIMethod.BOOTSTRAP && bootstrapReplicates < 1) {
            throw new IllegalArgumentException("bootstrapReplicates must be >= 1");
        }
        if (!(ciLevel > 0.0 && ciLevel < 1.0)) {
            throw new IllegalArgumentException("ciLevel must be in (0, 1)");
        }
        this.from = from;
        this.to = to;
        this.windowId = windowId;
        this.millisPerXUnit = millisPerXUnit;
        this.xAnchor = xAnchor;
        this.ciMethod = ciMethod;
        this.criticalValue = criticalValue;
        this.bootstrapReplicates = bootstrapReplicates;
        this.ciLevel = ciLevel;
        this.bootstrapSeed = bootstrapSeed;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
        if (ciMethod == CIMethod.BOOTSTRAP) {
            this.xs = new double[16];
            this.ys = new double[16];
        }
    }

    @Override
    public void addDataPoint(DataPoint dp) {
        hasInitialized = true;
        double x = (dp.getTimestamp() - xAnchor) / millisPerXUnit;
        double y = dp.getValue();
        sumX  += x;
        sumY  += y;
        sumXY += x * y;
        sumX2 += x * x;
        sumY2 += y * y;
        if (ciMethod == CIMethod.BOOTSTRAP) {
            if ((int) n == xs.length) {
                xs = Arrays.copyOf(xs, xs.length * 2);
                ys = Arrays.copyOf(ys, ys.length * 2);
            }
            xs[(int) n] = x;
            ys[(int) n] = y;
        }
        n++;
        computed = false;
    }

    @Override
    public void addAggregatedDataPoint(AggregatedDataPoint dataPoint) {
        throw new UnsupportedOperationException(
            "SampledOLSSketch consumes raw points via addDataPoint");
    }

    @Override
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty() || !(other instanceof SampledOLSSketch)) {
            return false;
        }
        return this.getTo() == other.getFrom();
    }

    @Override
    public Sketch combine(Sketch other) {
        if (!canCombineWith(other)) {
            LOG.debug("Cannot combine incompatible SampledOLSSketch instances");
            return this;
        }
        SampledOLSSketch o = (SampledOLSSketch) other;
        if (o.millisPerXUnit != this.millisPerXUnit || o.xAnchor != this.xAnchor
                || o.ciMethod != this.ciMethod) {
            throw new IllegalArgumentException("Cannot combine sketches with a different configuration");
        }
        if (ciMethod == CIMethod.BOOTSTRAP) {
            int total = (int) (this.n + o.n);
            if (total > xs.length) {
                int cap = Math.max(total, xs.length * 2);
                xs = Arrays.copyOf(xs, cap);
                ys = Arrays.copyOf(ys, cap);
            }
            System.arraycopy(o.xs, 0, xs, (int) this.n, (int) o.n);
            System.arraycopy(o.ys, 0, ys, (int) this.n, (int) o.n);
        }
        this.to     = o.to;
        this.n      += o.n;
        this.sumX   += o.sumX;
        this.sumY   += o.sumY;
        this.sumXY  += o.sumXY;
        this.sumX2  += o.sumX2;
        this.sumY2  += o.sumY2;
        this.hasInitialized |= o.hasInitialized;
        this.computed = false;
        return this;
    }

    private void compute() {
        computed = true;
        if (n < 2) {
            unbounded();
            return;
        }
        double nD = (double) n;
        double sxx = sumX2 - (sumX * sumX) / nD;
        double sxy = sumXY - (sumX * sumY) / nD;
        if (!(sxx > 0.0)) {
            unbounded();
            return;
        }
        slope = sxy / sxx;
        angle = Math.toDegrees(Math.atan(slope));
        if (ciMethod == CIMethod.BOOTSTRAP) {
            computeBootstrapInterval();
        } else {
            computeClosedFormInterval(nD, sxx, sxy);
        }
    }

    private void computeClosedFormInterval(double nD, double sxx, double sxy) {
        double syy = sumY2 - (sumY * sumY) / nD;
        double sse = Math.max(0.0, syy - slope * sxy);
        if (n <= 2) {
            slopeStdError = 0.0;
        } else {
            double residualVar = sse / (nD - 2.0);
            slopeStdError = Math.sqrt(residualVar / sxx);
        }
        double half = criticalValue * slopeStdError;
        minAngle = Math.toDegrees(Math.atan(slope - half));
        maxAngle = Math.toDegrees(Math.atan(slope + half));
    }

    private void computeBootstrapInterval() {
        int m = (int) n;
        Random rnd = new Random(bootstrapSeed);
        double[] slopes = new double[bootstrapReplicates];
        int valid = 0;
        for (int b = 0; b < bootstrapReplicates; b++) {
            double bx = 0, by = 0, bxy = 0, bx2 = 0;
            for (int i = 0; i < m; i++) {
                int idx = rnd.nextInt(m);
                double x = xs[idx];
                double y = ys[idx];
                bx += x;
                by += y;
                bxy += x * y;
                bx2 += x * x;
            }
            double rSxx = bx2 - (bx * bx) / m;
            if (rSxx > 0.0) {
                double rSxy = bxy - (bx * by) / m;
                slopes[valid++] = rSxy / rSxx;
            }
        }
        if (valid < Math.max(10, bootstrapReplicates / 10)) {
            unbounded();
            return;
        }
        double[] s = Arrays.copyOf(slopes, valid);
        Arrays.sort(s);
        double alpha = 1.0 - ciLevel;
        double lower = percentile(s, alpha / 2.0);
        double upper = percentile(s, 1.0 - alpha / 2.0);
        slopeStdError = stdDev(s);
        minAngle = Math.toDegrees(Math.atan(lower));
        maxAngle = Math.toDegrees(Math.atan(upper));
    }

    private static double percentile(double[] sorted, double p) {
        int idx = (int) Math.round(p * (sorted.length - 1));
        if (idx < 0) idx = 0;
        if (idx >= sorted.length) idx = sorted.length - 1;
        return sorted[idx];
    }

    private static double stdDev(double[] v) {
        double mean = 0;
        for (double x : v) mean += x;
        mean /= v.length;
        double ss = 0;
        for (double x : v) ss += (x - mean) * (x - mean);
        return Math.sqrt(ss / v.length);
    }

    private void unbounded() {
        slope = Double.POSITIVE_INFINITY;
        slopeStdError = Double.POSITIVE_INFINITY;
        angle = Double.POSITIVE_INFINITY;
        minAngle = -90.0;
        maxAngle = 90.0;
    }

    private void ensureComputed() {
        if (!computed) {
            compute();
        }
    }

    @Override
    public double getAngle() {
        ensureComputed();
        return angle;
    }

    @Override
    public double getMinAngle() {
        ensureComputed();
        return minAngle;
    }

    @Override
    public double getMaxAngle() {
        ensureComputed();
        return maxAngle;
    }

    @Override
    public double getAngleErrorMargin() {
        ensureComputed();
        double width = maxAngle - minAngle;
        if (!(width > 0) || Double.isNaN(width) || Double.isInfinite(width)) {
            return 0.0;
        }
        return Math.min(1.0, (width / 2.0) / 90.0);
    }

    public double getSlope() {
        ensureComputed();
        return slope;
    }

    public double getSlopeStdError() {
        ensureComputed();
        return slopeStdError;
    }

    public long getSampleCount() {
        return n;
    }

    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public boolean isEmpty() {
        return n == 0;
    }

    @Override
    public boolean hasInitialized() {
        return hasInitialized;
    }

    @Override
    public Sketch clone() {
        SampledOLSSketch s = new SampledOLSSketch(from, to, windowId, millisPerXUnit, xAnchor,
                ciMethod, criticalValue, bootstrapReplicates, ciLevel, bootstrapSeed);
        s.n = this.n;
        s.sumX = this.sumX;
        s.sumY = this.sumY;
        s.sumXY = this.sumXY;
        s.sumX2 = this.sumX2;
        s.sumY2 = this.sumY2;
        if (ciMethod == CIMethod.BOOTSTRAP) {
            s.xs = Arrays.copyOf(this.xs, this.xs.length);
            s.ys = Arrays.copyOf(this.ys, this.ys.length);
        }
        s.hasInitialized = this.hasInitialized;
        s.originalAggregateInterval = this.originalAggregateInterval;
        s.computed = false;
        return s;
    }

    @Override
    public String toString() {
        ensureComputed();
        return "SampledOLSSketch{from=" + from + ", to=" + to + ", n=" + n
                + ", ci=" + ciMethod + ", angle=" + angle
                + ", bound=[" + minAngle + ", " + maxAngle + "]}";
    }
}
