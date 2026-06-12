package gr.imsi.athenarc.middleware.sketch;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression coverage for {@link ApproxOLSSketch}.
 *
 * Tests live alongside {@link OLSSlopeBoundsTest}, which already covers the underlying
 * bound math. This file targets per-sketch behavior — angle computation triggered by
 * combine() *and* by matches() on a never-combined sketch (the single-bucket case that
 * silently failed before because the angle field defaulted to 0).
 */
public class ApproxOLSSketchTest {

    private static AggregatedDataPoint subBucket(long bucketFrom, long subFrom, long subTo,
                                                 double minValue, double maxValue) {
        Stats stats = new Stats() {
            @Override public int getCount() { return 1; }
            @Override public double getMinValue() { return minValue; }
            @Override public double getMaxValue() { return maxValue; }
            @Override public long getMinTimestamp() { return subFrom; }
            @Override public long getMaxTimestamp() { return subTo; }
            @Override public double getFirstValue() { return minValue; }
            @Override public long getFirstTimestamp() { return subFrom; }
            @Override public double getLastValue() { return maxValue; }
            @Override public long getLastTimestamp() { return subTo; }
            @Override public double getMean() { return (minValue + maxValue) / 2; }
            @Override public double getSum() { return (minValue + maxValue) / 2; }
        };
        return new AggregatedDataPoint() {
            @Override public int getMeasure() { return 0; }
            @Override public long getTimestamp() { return subFrom; }
            @Override public Stats getStats() { return stats; }
            @Override public long getFrom() { return subFrom; }
            @Override public long getTo() { return subTo; }
            @Override public double getValue() { throw new UnsupportedOperationException(); }
            @Override public int getCount() { return 1; }
        };
    }

    /** Multi-raw-point sub-bucket — needed for straddler counts &gt; 1 (the regime that
     *  exposes the Möbius bound's interior-k contribution). */
    private static AggregatedDataPoint multiSubBucket(long subFrom, long subTo, int count,
                                                      double sum, double minValue, double maxValue) {
        Stats stats = new Stats() {
            @Override public int getCount() { return count; }
            @Override public double getMinValue() { return minValue; }
            @Override public double getMaxValue() { return maxValue; }
            @Override public long getMinTimestamp() { return subFrom; }
            @Override public long getMaxTimestamp() { return subTo; }
            @Override public double getFirstValue() { return minValue; }
            @Override public long getFirstTimestamp() { return subFrom; }
            @Override public double getLastValue() { return maxValue; }
            @Override public long getLastTimestamp() { return subTo; }
            @Override public double getMean() { return sum / count; }
            @Override public double getSum() { return sum; }
        };
        return new AggregatedDataPoint() {
            @Override public int getMeasure() { return 0; }
            @Override public long getTimestamp() { return subFrom; }
            @Override public Stats getStats() { return stats; }
            @Override public long getFrom() { return subFrom; }
            @Override public long getTo() { return subTo; }
            @Override public double getValue() { throw new UnsupportedOperationException(); }
            @Override public int getCount() { return count; }
        };
    }

    /**
     * Single-bucket sketch: combine() is never called, but matches() must still produce
     * a meaningful angle — the bug we fixed by computing the angle lazily inside matches().
     */
    @Test
    public void singleBucketMatchesUsesLazyAngle() {
        // Bucket [0, 60_000) populated with 4 sub-buckets of width 15_000 ms,
        // values ramping +0.5 per sub-bucket (slope ~ +2/bucket -> atan ~ +63.4 deg).
        ApproxOLSSketch sketch = new ApproxOLSSketch(0L, 60_000L, /*windowId*/ 0);
        sketch.addAggregatedDataPoint(subBucket(0, 0,      15_000,  0.0,  0.5));
        sketch.addAggregatedDataPoint(subBucket(0, 15_000, 30_000,  0.5,  1.0));
        sketch.addAggregatedDataPoint(subBucket(0, 30_000, 45_000,  1.0,  1.5));
        sketch.addAggregatedDataPoint(subBucket(0, 45_000, 60_000,  1.5,  2.0));

        assertTrue("expected steep-up filter [40,80] to match",
                sketch.matches(ValueFilter.custom(40f, 80f)));
        assertFalse("expected steep-down filter [-80,-40] to not match",
                sketch.matches(ValueFilter.custom(-80f, -40f)));
        assertTrue("expected stable filter [-10,10] to not match",
                !sketch.matches(ValueFilter.custom(-10f, 10f)));
    }

    /**
     * Straddler-aware corner enumeration (v2). A left-straddler sub-bucket (one whose
     * absolute range starts before the sketch's segment start) triggers the
     * {@code cornerEnumBound} path: the LP runs with and without the straddler and
     * the union of envelopes is returned. The wider corner-enum envelope must
     * contain the strict-only envelope (rigor is preserved), and the basic-only
     * envelope must sit inside the corner-enum envelope (corner-enum union ⊇ any
     * single corner).
     */
    @Test
    public void cornerEnumProducesWiderEnvelopeWhenLeftStraddlerPresent() {
        // Strict-only sketch: just the 4 internal sub-buckets ramping up 0 -> 2.
        ApproxOLSSketch strict = new ApproxOLSSketch(0L, 60_000L, /*windowId*/ 0);
        strict.addAggregatedDataPoint(subBucket(0, 0,      15_000, 0.0, 0.5));
        strict.addAggregatedDataPoint(subBucket(0, 15_000, 30_000, 0.5, 1.0));
        strict.addAggregatedDataPoint(subBucket(0, 30_000, 45_000, 1.0, 1.5));
        strict.addAggregatedDataPoint(subBucket(0, 45_000, 60_000, 1.5, 2.0));
        double strictMin = strict.getMinAngle();
        double strictMax = strict.getMaxAngle();

        // Corner-enum sketch: same 4 internals + a LEFT-STRADDLER at [-15_000, 0]
        // whose values bias opposite (3.0..3.5) so including-vs-excluding the
        // straddler gives meaningfully different slope corners.
        ApproxOLSSketch withStraddler = new ApproxOLSSketch(0L, 60_000L, /*windowId*/ 0);
        withStraddler.addAggregatedDataPoint(subBucket(0, -15_000, 0,    3.0, 3.5));
        withStraddler.addAggregatedDataPoint(subBucket(0, 0,      15_000, 0.0, 0.5));
        withStraddler.addAggregatedDataPoint(subBucket(0, 15_000, 30_000, 0.5, 1.0));
        withStraddler.addAggregatedDataPoint(subBucket(0, 30_000, 45_000, 1.0, 1.5));
        withStraddler.addAggregatedDataPoint(subBucket(0, 45_000, 60_000, 1.5, 2.0));
        double cornerMin = withStraddler.getMinAngle();
        double cornerMax = withStraddler.getMaxAngle();

        // Rigor: corner-enum envelope is wider than strict-only (it's a union over
        // {include L} and {skip L}; skip L equals strict-only, include L is the
        // other corner).
        assertTrue("corner-enum lower must be ≤ strict-only lower (envelope widens)",
                cornerMin <= strictMin + 1e-9);
        assertTrue("corner-enum upper must be ≥ strict-only upper (envelope widens)",
                cornerMax >= strictMax - 1e-9);
    }

    /**
     * Regression guard for the {@code reserveCapacity} perf hint: pre-sizing the
     * backing arrays must NOT change the computed bound. Compares angle / min / max
     * for two sketches built from identical inputs — one with capacity reserved
     * up-front, one growing via the default resize cascade.
     */
    @Test
    public void reserveCapacityYieldsBitIdenticalBound() {
        // 32 sub-buckets — exceeds INITIAL_CAPACITY=16, so the no-reserve sketch
        // hits at least one resize cycle.
        int n = 32;
        ApproxOLSSketch noReserve = new ApproxOLSSketch(0L, 320_000L, /*windowId*/ 0);
        ApproxOLSSketch withReserve = new ApproxOLSSketch(0L, 320_000L, /*windowId*/ 0);
        withReserve.reserveCapacity(n);
        for (int i = 0; i < n; i++) {
            long subFrom = i * 10_000L;
            long subTo = (i + 1) * 10_000L;
            double lo = i * 0.1;
            double hi = i * 0.1 + 0.5;
            noReserve.addAggregatedDataPoint(subBucket(0, subFrom, subTo, lo, hi));
            withReserve.addAggregatedDataPoint(subBucket(0, subFrom, subTo, lo, hi));
        }
        assertTrue("getAngle must be bit-identical",
                Double.doubleToRawLongBits(noReserve.getAngle())
                        == Double.doubleToRawLongBits(withReserve.getAngle()));
        assertTrue("getMinAngle must be bit-identical",
                Double.doubleToRawLongBits(noReserve.getMinAngle())
                        == Double.doubleToRawLongBits(withReserve.getMinAngle()));
        assertTrue("getMaxAngle must be bit-identical",
                Double.doubleToRawLongBits(noReserve.getMaxAngle())
                        == Double.doubleToRawLongBits(withReserve.getMaxAngle()));
    }

    /**
     * Bit-identity guard for the dp{Mid,HalfWidth,Mean,Ydev} precompute refactor in
     * {@code weightedMeanBound}. The production path now reads precomputed per-sub-bucket
     * constants instead of recomputing them in pass-1/pass-2. This test rebuilds the
     * exact same arithmetic the old inline version did and asserts the production
     * angle/minAngle/maxAngle are bit-identical.
     */
    @Test
    public void precomputedConstantsYieldBitIdenticalBoundVsInlineRecompute() {
        // 64 sub-buckets with varied y, count=1 each (so a single weightedMeanBound
        // call covers the whole composite — no straddler / no cornerEnum branching).
        int n = 64;
        long bucketMs = 1_000_000L;
        ApproxOLSSketch sk = new ApproxOLSSketch(0L, n * bucketMs, /*windowId*/ 0);
        double[] mid = new double[n], halfW = new double[n], mean = new double[n],
                ydev = new double[n], cnt = new double[n];
        for (int i = 0; i < n; i++) {
            long subFrom = i * bucketMs;
            long subTo = (i + 1) * bucketMs;
            // Asymmetric min/max so dpYdev branches both ways across the run.
            double lo = i * 0.13 - 1.5;
            double hi = i * 0.17 + 2.7;
            sk.addAggregatedDataPoint(subBucket(0, subFrom, subTo, lo, hi));
            // Mirror addAggregatedDataPoint's relative-coord arithmetic exactly.
            double unitMs = (double) (n * bucketMs);
            double fromRel = subFrom / unitMs;
            double toRel = subTo / unitMs;
            mid[i] = (fromRel + toRel) * 0.5;
            halfW[i] = (toRel - fromRel) * 0.5;
            cnt[i] = 1.0;
            double sum = (lo + hi) / 2;       // subBucket helper sets stats.getSum() = (min+max)/2
            mean[i] = sum / cnt[i];
            ydev[i] = Math.max(hi - mean[i], mean[i] - lo);
        }
        // Independent re-derivation of weightedMeanBound using inline arithmetic.
        long N = n; // count=1 per sub-bucket
        double sumNmid = 0, sumY = 0, sumNhalfW = 0;
        for (int i = 0; i < n; i++) {
            sumNmid += cnt[i] * mid[i];
            sumY += cnt[i] * mean[i];
            sumNhalfW += cnt[i] * halfW[i];
        }
        double Xbar = sumNmid / N, Ybar = sumY / N, deltaMax = sumNhalfW / N;
        double sxxEst = 0, sxyEst = 0, eXy = 0, eXxDown = 0, eXxUpExtra = 0;
        for (int i = 0; i < n; i++) {
            double hw = halfW[i], width = hw + hw;
            double devX = mid[i] - Xbar, devY = mean[i] - Ybar;
            sxxEst += cnt[i] * devX * devX;
            sxyEst += cnt[i] * devX * devY;
            eXy += cnt[i] * Math.abs(devY) * hw + cnt[i] * width * ydev[i];
            eXxDown += cnt[i] * Math.abs(devX) * width;
            double sp = hw + deltaMax;
            eXxUpExtra += cnt[i] * sp * sp + cnt[i] * hw * hw;
        }
        double slopeEst = sxyEst / sxxEst;
        double sxxLo = sxxEst - eXxDown, sxxHi = sxxEst + eXxDown + eXxUpExtra;
        double sxyLo = sxyEst - eXy, sxyHi = sxyEst + eXy;
        double q1 = sxyLo / sxxLo, q2 = sxyLo / sxxHi, q3 = sxyHi / sxxLo, q4 = sxyHi / sxxHi;
        double slopeLo = Math.min(Math.min(q1, q2), Math.min(q3, q4));
        double slopeHi = Math.max(Math.max(q1, q2), Math.max(q3, q4));
        double expectedAngle = Math.toDegrees(Math.atan(slopeEst));
        double expectedMin = Math.toDegrees(Math.atan(slopeLo));
        double expectedMax = Math.toDegrees(Math.atan(slopeHi));

        assertTrue("angle bit-identical to inline recompute",
                Double.doubleToRawLongBits(sk.getAngle())
                        == Double.doubleToRawLongBits(expectedAngle));
        assertTrue("minAngle bit-identical to inline recompute",
                Double.doubleToRawLongBits(sk.getMinAngle())
                        == Double.doubleToRawLongBits(expectedMin));
        assertTrue("maxAngle bit-identical to inline recompute",
                Double.doubleToRawLongBits(sk.getMaxAngle())
                        == Double.doubleToRawLongBits(expectedMax));
    }

    /**
     * Rigorous Möbius straddler bound (part2-proof §"The Möbius construction") on the
     * proof's own worked example. Straddler L has count=4, sum=88, y-range [16, 28] on
     * absolute span [6, 14) crossing T₀=10; internals A, B inside. Points lie on
     * y = 2x + 2 so the true absolute slope is 2; the bound must enclose it.
     */
    @Test
    public void mobiusBoundEnclosesTrueSlopeOnPart2ProofExample() {
        boolean prev = ApproxOLSSketch.RIGOROUS_STRADDLER_BOUND;
        ApproxOLSSketch.RIGOROUS_STRADDLER_BOUND = true;
        try {
            // Segment [10, 30] → unitMs=20 → relative slope = absolute slope * 20.
            // True abs slope = 2.0 → relative slope = 40 → angle ≈ atan(40)° ≈ 88.57°.
            ApproxOLSSketch sk = new ApproxOLSSketch(10L, 30L, /*windowId*/ 0);
            sk.addAggregatedDataPoint(multiSubBucket(6,  14, 4,  88.0, 16.0, 28.0));  // straddler L
            sk.addAggregatedDataPoint(multiSubBucket(14, 18, 3, 102.0, 33.6, 34.4));  // internal A
            sk.addAggregatedDataPoint(multiSubBucket(22, 26, 3, 150.0, 49.6, 50.4));  // internal B
            double minA = sk.getMinAngle();
            double maxA = sk.getMaxAngle();
            double trueAng = Math.toDegrees(Math.atan(2.0 * 20.0)); // ≈ 88.567°
            assertTrue("min angle " + minA + " must be ≤ true angle " + trueAng,
                    minA <= trueAng + 1e-6);
            assertTrue("max angle " + maxA + " must be ≥ true angle " + trueAng,
                    maxA >= trueAng - 1e-6);
        } finally {
            ApproxOLSSketch.RIGOROUS_STRADDLER_BOUND = prev;
        }
    }

    /**
     * Combine path: two consecutive sketches with opposing slopes — confirm the combined
     * sketch's angle is the bound of the merged data points (not just the first sketch's slope).
     */
    @Test
    public void combineProducesBoundOverMergedPoints() {
        // First bucket: ramp up 0 -> 2.
        ApproxOLSSketch a = new ApproxOLSSketch(0L, 60_000L, 0);
        a.addAggregatedDataPoint(subBucket(0, 0,      15_000, 0.0, 0.5));
        a.addAggregatedDataPoint(subBucket(0, 15_000, 30_000, 0.5, 1.0));
        a.addAggregatedDataPoint(subBucket(0, 30_000, 45_000, 1.0, 1.5));
        a.addAggregatedDataPoint(subBucket(0, 45_000, 60_000, 1.5, 2.0));

        // Second bucket (consecutive): ramp down 2 -> 0.
        ApproxOLSSketch b = new ApproxOLSSketch(60_000L, 120_000L, 1);
        b.addAggregatedDataPoint(subBucket(60_000, 60_000,  75_000,  1.5, 2.0));
        b.addAggregatedDataPoint(subBucket(60_000, 75_000,  90_000,  1.0, 1.5));
        b.addAggregatedDataPoint(subBucket(60_000, 90_000,  105_000, 0.5, 1.0));
        b.addAggregatedDataPoint(subBucket(60_000, 105_000, 120_000, 0.0, 0.5));

        a.combine(b);

        // Net slope across [0, 2, 0] is ~0; bound widens to include both sides.
        // Angle (midpoint of bounds) should sit near 0 with substantial error margin.
        assertTrue("combined angle near zero but bound straddles steep up/down",
                Math.abs(a.getAngle()) < 30.0);
        assertTrue("min angle should be negative",  a.getMinAngle() < -10.0);
        assertTrue("max angle should be positive",  a.getMaxAngle() >  10.0);
    }
}
