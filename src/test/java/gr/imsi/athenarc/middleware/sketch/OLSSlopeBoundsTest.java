package gr.imsi.athenarc.middleware.sketch;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the closed-form OLS slope bounds under per-bucket interval uncertainty in y.
 *
 * The bound is tight under bucket-midpoint x. The tests check:
 *  1. Containment: random y_i sampled inside [yMin_i, yMax_i] yield an OLS slope within [lower, upper].
 *  2. Tightness: the bound's lower / upper is exactly attained by the optimal vertex picks.
 *  3. Adversarial regression case: two buckets with opposing min/max produce the full feasible slope range.
 *  4. Edge cases: n < 2 and degenerate-x return undefined.
 */
public class OLSSlopeBoundsTest {

    private static final double TOL = 1e-9;

    private static double olsSlope(double[] x, double[] y) {
        SimpleRegression r = new SimpleRegression(true);
        for (int i = 0; i < x.length; i++) {
            r.addData(x[i], y[i]);
        }
        return r.getSlope();
    }

    @Test
    public void containment_random100() {
        Random rng = new Random(0xC0FFEEL);
        for (int trial = 0; trial < 100; trial++) {
            int n = 3 + rng.nextInt(18);
            double[] x = new double[n];
            double[] yMin = new double[n];
            double[] yMax = new double[n];
            double[] yTrue = new double[n];
            for (int i = 0; i < n; i++) {
                x[i] = i + rng.nextDouble();
                double a = rng.nextGaussian() * 5.0;
                double b = a + rng.nextDouble() * 3.0;
                yMin[i] = a;
                yMax[i] = b;
                yTrue[i] = yMin[i] + rng.nextDouble() * (yMax[i] - yMin[i]);
            }
            OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(x, yMin, yMax);
            assertTrue("trial " + trial + " expected defined bounds", bounds.defined);
            double trueSlope = olsSlope(x, yTrue);
            assertTrue(
                    "trial " + trial + ": true slope " + trueSlope
                            + " not in [" + bounds.slopeLower + ", " + bounds.slopeUpper + "]",
                    trueSlope >= bounds.slopeLower - TOL && trueSlope <= bounds.slopeUpper + TOL);
        }
    }

    @Test
    public void tightness_attainedAtOptimalVertex() {
        Random rng = new Random(42L);
        for (int trial = 0; trial < 50; trial++) {
            int n = 3 + rng.nextInt(10);
            double[] x = new double[n];
            double[] yMin = new double[n];
            double[] yMax = new double[n];
            for (int i = 0; i < n; i++) {
                x[i] = i + rng.nextDouble();
                double a = rng.nextGaussian() * 5.0;
                double b = a + 0.5 + rng.nextDouble() * 3.0;
                yMin[i] = a;
                yMax[i] = b;
            }
            double meanX = 0.0;
            for (double v : x) meanX += v;
            meanX /= n;

            double[] yUpperPick = new double[n];
            double[] yLowerPick = new double[n];
            for (int i = 0; i < n; i++) {
                double xc = x[i] - meanX;
                if (xc > 0) {
                    yUpperPick[i] = yMax[i];
                    yLowerPick[i] = yMin[i];
                } else if (xc < 0) {
                    yUpperPick[i] = yMin[i];
                    yLowerPick[i] = yMax[i];
                } else {
                    double mid = 0.5 * (yMin[i] + yMax[i]);
                    yUpperPick[i] = mid;
                    yLowerPick[i] = mid;
                }
            }
            double upperVertexSlope = olsSlope(x, yUpperPick);
            double lowerVertexSlope = olsSlope(x, yLowerPick);

            OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(x, yMin, yMax);
            assertEquals("trial " + trial + " upper bound matches optimal vertex slope",
                    upperVertexSlope, bounds.slopeUpper, 1e-9);
            assertEquals("trial " + trial + " lower bound matches optimal vertex slope",
                    lowerVertexSlope, bounds.slopeLower, 1e-9);
            assertTrue("lower ≤ upper", bounds.slopeLower <= bounds.slopeUpper + TOL);
        }
    }

    @Test
    public void adversarialTwoBuckets_recoverFullSlopeRange() {
        // x at 0 and 1, y in [0, 10] in both buckets.
        // Feasible slope range over all y choices is [-10, +10].
        // The legacy "all-min vs all-max" heuristic returned [0, 0].
        double[] x = {0.0, 1.0};
        double[] yMin = {0.0, 0.0};
        double[] yMax = {10.0, 10.0};
        OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(x, yMin, yMax);
        assertTrue(bounds.defined);
        assertEquals(-10.0, bounds.slopeLower, TOL);
        assertEquals(10.0, bounds.slopeUpper, TOL);
    }

    @Test
    public void monotonicSeries_zeroWidthIntervals_recoverExactSlope() {
        double[] x = {0, 1, 2, 3, 4};
        double[] y = {0, 2, 4, 6, 8}; // exact slope = 2
        OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(x, y, y);
        assertTrue(bounds.defined);
        assertEquals(2.0, bounds.slopeLower, TOL);
        assertEquals(2.0, bounds.slopeUpper, TOL);
    }

    @Test
    public void undefined_singleBucket() {
        OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(new double[]{1.0}, new double[]{0.0}, new double[]{1.0});
        assertFalse(bounds.defined);
    }

    @Test
    public void undefined_emptyInput() {
        OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(new double[0], new double[0], new double[0]);
        assertFalse(bounds.defined);
    }

    @Test
    public void undefined_allXEqual() {
        double[] x = {1.0, 1.0, 1.0};
        double[] yMin = {0.0, 0.0, 0.0};
        double[] yMax = {1.0, 1.0, 1.0};
        OLSSlopeBounds.Result bounds = OLSSlopeBounds.compute(x, yMin, yMax);
        assertFalse(bounds.defined);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsLengthMismatch() {
        OLSSlopeBounds.compute(new double[]{0, 1}, new double[]{0}, new double[]{1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNullInput() {
        OLSSlopeBounds.compute(null, new double[]{0}, new double[]{1});
    }
}
