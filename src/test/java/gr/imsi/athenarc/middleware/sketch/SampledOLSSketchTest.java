package gr.imsi.athenarc.middleware.sketch;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import gr.imsi.athenarc.middleware.domain.DataPoint;

/**
 * Tests the sampled OLS slope estimator.
 *  1. A full sample reproduces the exact OLS slope/angle over the same x-mapping.
 *  2. A perfect line yields a zero-width angle interval.
 *  3. The closed-form interval brackets the point estimate and has positive width under noise.
 *  4. combine() over two consecutive sketches equals the OLS over the union.
 *  5. The bootstrap interval is reproducible for a fixed seed and has positive width.
 *  6. A single point is reported unbounded.
 */
public class SampledOLSSketchTest {

    private static final double TOL = 1e-9;
    private static final long UNIT_MS = 60_000L;
    private static final long DT_MS = 1_000L;

    private static final class P implements DataPoint {
        final long t;
        final double v;
        P(long t, double v) { this.t = t; this.v = v; }
        @Override public long getTimestamp() { return t; }
        @Override public double getValue() { return v; }
        @Override public int getMeasure() { return 0; }
    }

    private static double x(long t) {
        return t / (double) UNIT_MS;
    }

    @Test
    public void fullSampleMatchesExactOls() {
        SampledOLSSketch sk = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L, 1.96);
        SimpleRegression ref = new SimpleRegression(true);
        Random r = new Random(1);
        for (int i = 0; i < 60; i++) {
            long t = i * DT_MS;
            double y = 3.0 + 0.5 * x(t) + r.nextGaussian() * 0.01;
            ref.addData(x(t), y);
            sk.addDataPoint(new P(t, y));
        }
        assertEquals(ref.getSlope(), sk.getSlope(), TOL);
        assertEquals(Math.toDegrees(Math.atan(ref.getSlope())), sk.getAngle(), TOL);
    }

    @Test
    public void perfectLineHasZeroWidthInterval() {
        double b = 0.5;
        SampledOLSSketch sk = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L, 1.96);
        for (int i = 0; i < 60; i++) {
            long t = i * DT_MS;
            sk.addDataPoint(new P(t, 2.0 + b * x(t)));
        }
        assertEquals(Math.toDegrees(Math.atan(b)), sk.getAngle(), 1e-3);
        assertEquals(sk.getMinAngle(), sk.getMaxAngle(), 1e-3);
    }

    @Test
    public void closedFormIntervalBracketsEstimate() {
        SampledOLSSketch sk = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L, 1.96);
        Random r = new Random(5);
        for (int i = 0; i < 60; i++) {
            long t = i * DT_MS;
            double y = 0.4 * x(t) + r.nextGaussian() * 0.1;
            sk.addDataPoint(new P(t, y));
        }
        assertTrue(sk.getMinAngle() <= sk.getAngle());
        assertTrue(sk.getAngle() <= sk.getMaxAngle());
        assertTrue(sk.getMaxAngle() - sk.getMinAngle() > 0);
    }

    @Test
    public void combineEqualsUnion() {
        SampledOLSSketch a = new SampledOLSSketch(0, 30 * DT_MS, 0, (double) UNIT_MS, 0L, 1.96);
        SampledOLSSketch b = new SampledOLSSketch(30 * DT_MS, 60 * DT_MS, 1, (double) UNIT_MS, 0L, 1.96);
        SimpleRegression ref = new SimpleRegression(true);
        Random r = new Random(2);
        for (int i = 0; i < 60; i++) {
            long t = i * DT_MS;
            double y = 1.0 + 0.3 * x(t) + r.nextGaussian() * 0.02;
            ref.addData(x(t), y);
            (i < 30 ? a : b).addDataPoint(new P(t, y));
        }
        a.combine(b);
        assertEquals(ref.getSlope(), a.getSlope(), TOL);
        assertEquals(60, a.getSampleCount());
    }

    @Test
    public void bootstrapIntervalIsReproducibleAndPositiveWidth() {
        SampledOLSSketch s1 = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L,
                SampledOLSSketch.DEFAULT_BOOTSTRAP_REPLICATES, 0.95, 7L);
        SampledOLSSketch s2 = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L,
                SampledOLSSketch.DEFAULT_BOOTSTRAP_REPLICATES, 0.95, 7L);
        Random r = new Random(3);
        for (int i = 0; i < 60; i++) {
            long t = i * DT_MS;
            double y = 0.2 * x(t) + r.nextGaussian() * 0.05;
            s1.addDataPoint(new P(t, y));
            s2.addDataPoint(new P(t, y));
        }
        assertEquals(s1.getMinAngle(), s2.getMinAngle(), TOL);
        assertEquals(s1.getMaxAngle(), s2.getMaxAngle(), TOL);
        assertTrue(s1.getMaxAngle() - s1.getMinAngle() > 0);
    }

    @Test
    public void singlePointIsUnbounded() {
        SampledOLSSketch sk = new SampledOLSSketch(0, 60 * DT_MS, 0, (double) UNIT_MS, 0L, 1.96);
        sk.addDataPoint(new P(0, 5.0));
        assertEquals(-90.0, sk.getMinAngle(), 0.0);
        assertEquals(90.0, sk.getMaxAngle(), 0.0);
        assertTrue(Double.isInfinite(sk.getAngle()));
    }
}
