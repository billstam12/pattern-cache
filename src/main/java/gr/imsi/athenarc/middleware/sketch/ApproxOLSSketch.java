package gr.imsi.athenarc.middleware.sketch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Only for non timestampe stats = agg. time series spans */
public class ApproxOLSSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(ApproxOLSSketch.class);

    private long from;
    private long to;
    private long windowId;
    
    private double angle;
    private double minAngle; // Lower bound of angle error
    private double maxAngle; // Upper bound of angle error
    private double angleErrorMargin; // The difference between max and min angles
    private boolean angleComputed = false;

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // Parallel-array storage for sub-bucket reference points. Chosen over a
    // List<ReferenceDataPoint> so the LP loop iterates flat double[] arrays
    // (cache-friendly, no per-element pointer dereference) and so the BFS
    // sliding-window path can advance {@code head} in O(1) instead of
    // memcpy'ing a sub-list.
    private double[] dpFrom;
    private double[] dpTo;
    private double[] dpMin;
    private double[] dpMax;
    private double[] dpSum;
    private int[] dpCount;
    // Per-sub-bucket constants precomputed at add time. These were recomputed
    // every LP call before — profiling showed weightedMeanBound at ~94% of
    // slow-query wall time, and ~30% of each pass-2 iteration was just
    // recomputing these from dpFrom/dpTo/dpSum/dpCount/dpMin/dpMax. Storing
    // them lets pass-2 read instead of recompute. Bit-identical: same
    // arithmetic, same values, just done at add time instead of LP time.
    private double[] dpMid;       // (dpFrom + dpTo) * 0.5
    private double[] dpHalfWidth; // (dpTo - dpFrom) * 0.5
    private double[] dpMean;      // dpSum / dpCount
    private double[] dpYdev;      // max(dpMax - dpMean, dpMean - dpMin)
    /** First valid index in the parallel arrays — slide-remove advances this. */
    private int head;
    /** One past the last valid index — append advances this. */
    private int tail;

    /** Pre-sizes for a typical aggFactor up to 16 so single-sketch population skips
     *  the 4→8→16 cascade in {@link #ensureCapacity}. Pure perf knob — array
     *  contents and the read path are unchanged, so bounds are bit-identical. */
    private static final int INITIAL_CAPACITY = 16;

    /** Use the Möbius/decoupled-box straddler bound (rigorous, part2-proof §"The Möbius
     *  construction") instead of the cheap 2-corner endpoint heuristic in
     *  {@link #cornerEnumBound}. Off by default to preserve current perf. */
    public static boolean RIGOROUS_STRADDLER_BOUND = false;

    private AggregateInterval originalAggregateInterval;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param windowId The window id of this sketch, used to calculate the angle
     */
    public ApproxOLSSketch(long from, long to, long windowId) {
        this.from = from;
        this.to = to;
        this.windowId = windowId;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
        this.dpFrom = new double[INITIAL_CAPACITY];
        this.dpTo = new double[INITIAL_CAPACITY];
        this.dpMin = new double[INITIAL_CAPACITY];
        this.dpMax = new double[INITIAL_CAPACITY];
        this.dpSum = new double[INITIAL_CAPACITY];
        this.dpCount = new int[INITIAL_CAPACITY];
        this.dpMid = new double[INITIAL_CAPACITY];
        this.dpHalfWidth = new double[INITIAL_CAPACITY];
        this.dpMean = new double[INITIAL_CAPACITY];
        this.dpYdev = new double[INITIAL_CAPACITY];
        this.head = 0;
        this.tail = 0;
    }

    private void ensureCapacity(int needed) {
        if (dpFrom.length >= needed) return;
        int cap = Math.max(dpFrom.length << 1, needed);
        dpFrom = Arrays.copyOf(dpFrom, cap);
        dpTo = Arrays.copyOf(dpTo, cap);
        dpMin = Arrays.copyOf(dpMin, cap);
        dpMax = Arrays.copyOf(dpMax, cap);
        dpSum = Arrays.copyOf(dpSum, cap);
        dpCount = Arrays.copyOf(dpCount, cap);
        dpMid = Arrays.copyOf(dpMid, cap);
        dpHalfWidth = Arrays.copyOf(dpHalfWidth, cap);
        dpMean = Arrays.copyOf(dpMean, cap);
        dpYdev = Arrays.copyOf(dpYdev, cap);
    }

    /**
     * Pre-size for {@code expectedTotalSubBuckets} active sub-bucket points (i.e.
     * the size after a planned batch of {@link #addAggregatedDataPoint} or
     * {@link #combine} calls). Skips the resize cascade by jumping straight to
     * the final array length. Bit-identical bound semantics — only the array's
     * backing capacity changes; reads still iterate {@code [head, tail)}.
     */
    @Override
    public void reserveCapacity(int expectedTotalSubBuckets) {
        if (expectedTotalSubBuckets <= 0) return;
        // head + expected = needed array length (head dead-space stays untouched).
        ensureCapacity(head + expectedTotalSubBuckets);
    }

    /**
     * Adds an aggregated data point to this sketch, using the configured aggregation type.
     *
     * @param dp The aggregated data point to add
     */
    @Override
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true; // Mark as having underlying data
        Stats stats = dp.getStats();
        if (stats.getCount() > 0) {
            double unitMs = originalAggregateInterval.toDuration().toMillis();
            double fromPositionRelativeToSketch = windowId + (dp.getFrom() - this.from) / unitMs;
            double toPositionRelativeToSketch = windowId + (dp.getTo() - this.from) / unitMs;
            ensureCapacity(tail + 1);
            double minV = stats.getMinValue();
            double maxV = stats.getMaxValue();
            double sumV = stats.getSum();
            int cnt = stats.getCount();
            double meanV = sumV / cnt;
            // The arithmetic here MUST mirror exactly what weightedMeanBound /
            // evalDecoupledBound did inline before, so the stored values are
            // bit-identical to the on-the-fly recomputation they replace.
            dpFrom[tail] = fromPositionRelativeToSketch;
            dpTo[tail] = toPositionRelativeToSketch;
            dpMin[tail] = minV;
            dpMax[tail] = maxV;
            dpSum[tail] = sumV;
            dpCount[tail] = cnt;
            dpMid[tail] = (fromPositionRelativeToSketch + toPositionRelativeToSketch) * 0.5;
            dpHalfWidth[tail] = (toPositionRelativeToSketch - fromPositionRelativeToSketch) * 0.5;
            dpMean[tail] = meanV;
            dpYdev[tail] = Math.max(maxV - meanV, meanV - minV);
            tail++;
        }
    }
    
    /**
     * Checks if this sketch can be combined with another one.
     * The sketches must be consecutive (this.to == other.from) and have compatible aggregation types.
     * 
     * @param other The sketch to check for compatibility
     * @return true if sketches can be combined, false otherwise
     */
    @Override
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            LOG.debug("Cannot combine with null or empty sketch");
            return false;
        }
        
        if (!(other instanceof ApproxOLSSketch)) {
            LOG.debug("Cannot combine sketches of different types: {}", other.getClass());
            return false;
        }
        
        if (this.getTo() != other.getFrom()) {
            LOG.debug("Cannot combine non-consecutive sketches. Current sketch ends at {} but next sketch starts at {}", 
                      this.getTo(), other.getFrom());
            return false;
        }
        
        return true;
    }
    
    /**
     * Combines this sketch with another one, extending the time interval and updating stats.
     * The sketches must be consecutive (this.to == other.from).
     * 
     * @param other The sketch to combine with this one
     * @return This sketch after combination (for method chaining)
     * @throws IllegalArgumentException if the sketches are not compatible
     */
    @Override
    public Sketch combine(Sketch other) {
        // Validate input using canCombineWith
        if (!canCombineWith(other)) {
            throw new IllegalArgumentException("Cannot combine incompatible sketches");
        }

        ApproxOLSSketch otherSketch = (ApproxOLSSketch) other;

        this.to = otherSketch.getTo();
        int otherHead = otherSketch.head;
        int n = otherSketch.tail - otherHead;
        // D2 seam dedupe: if a boundary straddler was routed to both this and
        // other (relaxed-cache-reuse populate), it sits as this's rightmost AND
        // other's leftmost sub-bucket with identical relative (dpFrom, dpTo) —
        // the relative-coord formula at addAggregatedDataPoint() makes the two
        // representations bit-equal across consecutive outer buckets. Skip
        // other's first entry in that case so the chained list stays gap-free
        // overlap-free (hypothesis (H5) of approxols-bound-proof.md).
        if (n > 0 && tail - head > 0
                && dpFrom[tail - 1] == otherSketch.dpFrom[otherHead]
                && dpTo[tail - 1] == otherSketch.dpTo[otherHead]) {
            otherHead++;
            n--;
        }
        if (n > 0) {
            ensureCapacity(tail + n);
            System.arraycopy(otherSketch.dpFrom,      otherHead, dpFrom,      tail, n);
            System.arraycopy(otherSketch.dpTo,        otherHead, dpTo,        tail, n);
            System.arraycopy(otherSketch.dpMin,       otherHead, dpMin,       tail, n);
            System.arraycopy(otherSketch.dpMax,       otherHead, dpMax,       tail, n);
            System.arraycopy(otherSketch.dpSum,       otherHead, dpSum,       tail, n);
            System.arraycopy(otherSketch.dpCount,     otherHead, dpCount,     tail, n);
            System.arraycopy(otherSketch.dpMid,       otherHead, dpMid,       tail, n);
            System.arraycopy(otherSketch.dpHalfWidth, otherHead, dpHalfWidth, tail, n);
            System.arraycopy(otherSketch.dpMean,      otherHead, dpMean,      tail, n);
            System.arraycopy(otherSketch.dpYdev,      otherHead, dpYdev,      tail, n);
            tail += n;
        }
        // Defer the LP bound calc — callers in findPossibleMatches chain
        // combine() N times and only read the angle once at the end, so
        // recomputing eagerly per step was O(count²) in LP work.
        this.angleComputed = false;
        return this;
    }

    @Override
    public boolean supportsSliding() {
        return true;
    }

    @Override
    public boolean supportsRelaxedAlignment() {
        return true;
    }

    /** Number of underlying sub-bucket points currently in the composite. */
    @Override
    public int dataPointCount() {
        return tail - head;
    }

    /**
     * Slide-removal of the {@code n} oldest sub-bucket points (the front chunk
     * contributed by the sketch that's leaving the window). Updates {@code from}
     * to the new front sketch's start so {@link #canCombineWith} on the leading
     * edge continues to work, and invalidates the cached angle. O(1) — just
     * advances {@code head}; the dropped slots stay in the underlying arrays
     * until the next combine grows past them, at which point they're harmless
     * dead space ahead of {@code head}. Never call from sketch construction.
     */
    @Override
    public void removeFrontDataPoints(int n, long newFrom) {
        int sz = tail - head;
        if (n < 0 || n > sz) {
            throw new IllegalArgumentException("removeFront " + n + " out of [0, " + sz + "]");
        }
        head += n;
        this.from = newFrom;
        this.angleComputed = false;
    }
    

    // Accessors and utility methods
    
  
    @Override
    public long getFrom() {
        return from;
    }
    
    @Override
    public long getTo() {
        return to;
    }

    @Override
    public double getAngle() {
        ensureAngleComputed();
        return angle;
    }

    /**
     * Gets the minimum possible angle based on error bounds calculation.
     *
     * @return The minimum possible angle value
     */
    public double getMinAngle() {
        ensureAngleComputed();
        return minAngle;
    }

    /**
     * Gets the maximum possible angle based on error bounds calculation.
     *
     * @return The maximum possible angle value
     */
    public double getMaxAngle() {
        ensureAngleComputed();
        return maxAngle;
    }

    /**
     * Gets the error margin in the angle calculation.
     *
     * @return The error margin (difference between max and min angles)
     */
    public double getAngleErrorMargin() {
        ensureAngleComputed();
        return angleErrorMargin;
    }

    private void ensureAngleComputed() {
        if (!angleComputed) {
            if (gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch.PROFILE_ENABLED) {
                long lpStart = System.nanoTime();
                calculateMinMaxAngle();
                gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch.PROFILE_LP_NS += System.nanoTime() - lpStart;
                gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch.PROFILE_LP_CALLS++;
            } else {
                calculateMinMaxAngle();
            }
            angleComputed = true;
        }
    }



    @Override
    public Sketch clone() {
        ApproxOLSSketch sketch = new ApproxOLSSketch(this.from, this.to, this.windowId);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.minAngle = this.minAngle;
        sketch.maxAngle = this.maxAngle;
        sketch.angleErrorMargin = this.angleErrorMargin;
        sketch.angleComputed = this.angleComputed;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        int sz = tail - head;
        sketch.ensureCapacity(sz);
        if (sz > 0) {
            System.arraycopy(this.dpFrom,      head, sketch.dpFrom,      0, sz);
            System.arraycopy(this.dpTo,        head, sketch.dpTo,        0, sz);
            System.arraycopy(this.dpMin,       head, sketch.dpMin,       0, sz);
            System.arraycopy(this.dpMax,       head, sketch.dpMax,       0, sz);
            System.arraycopy(this.dpSum,       head, sketch.dpSum,       0, sz);
            System.arraycopy(this.dpCount,     head, sketch.dpCount,     0, sz);
            System.arraycopy(this.dpMid,       head, sketch.dpMid,       0, sz);
            System.arraycopy(this.dpHalfWidth, head, sketch.dpHalfWidth, 0, sz);
            System.arraycopy(this.dpMean,      head, sketch.dpMean,      0, sz);
            System.arraycopy(this.dpYdev,      head, sketch.dpYdev,      0, sz);
        }
        sketch.head = 0;
        sketch.tail = sz;
        return sketch;
    }

    @Override
    public boolean isEmpty() {
        return head == tail;
    }

    @Override
    public boolean hasInitialized() {
        return hasInitialized;
    }
    
    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
    }
    
    /**
     * Computes the slope of a composite sketch against the ValueFilter of a segment.
     * Returns true if the slope is within the filter's range.
     */
    public boolean matches(ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        ensureAngleComputed();
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return angle >= low && angle <= high;
    }

   
    /**
     * Point estimate + rigorous envelope for the raw-sample OLS slope of this
     * calendar-bucketed segment, from per-sub-bucket x-range [from_i, to_i],
     * y-range [min_i, max_i], mean_i and count_i. Iterates the parallel arrays
     * over [{@code head}, {@code tail}) directly — no scratch arrays allocated.
     * See {@link #weightedMeanBound}.
     */
    private void calculateMinMaxAngle() {
        int sz = tail - head;
        if (sz < 2) {
            markUndefined();
            return;
        }

        long N = 0L;
        for (int i = head; i < tail; i++) {
            int realCount = dpCount[i];
            if (realCount <= 0) {
                throw new IllegalStateException(
                        "ApproxOLS bound needs a real raw-sample count per sub-bucket, "
                                + "but sub-bucket " + (i - head) + " reports count=" + realCount
                                + ". Make sure 'count' is in the aggregate-functions set requested "
                                + "from the data source (see AggregationFunctionsConfig).");
            }
            if (Double.isNaN(dpSum[i])) {
                throw new IllegalStateException(
                        "ApproxOLS bound needs a per-sub-bucket sum, but sub-bucket " + (i - head)
                                + " has none. Make sure 'sum' is in the aggregate-functions set "
                                + "requested from the data source (see AggregationFunctionsConfig).");
            }
            N += realCount;
        }
        if (N < 2) {
            markUndefined();
            return;
        }

        boolean leftStrad = hasLeftStraddler();
        boolean rightStrad = hasRightStraddler();
        double[] bounds;
        if (leftStrad || rightStrad) {
            bounds = RIGOROUS_STRADDLER_BOUND
                    ? mobiusStraddlerBound(leftStrad, rightStrad)
                    : cornerEnumBound(N, leftStrad, rightStrad);
        } else {
            bounds = weightedMeanBound(head, tail, N);
        }

        double slopeLower = bounds[0];
        double slopeEstimate = bounds[1];
        double slopeUpper = bounds[2];

        if (Double.isNaN(slopeLower) || Double.isNaN(slopeEstimate) || Double.isNaN(slopeUpper)
                || Double.isInfinite(slopeLower) || Double.isInfinite(slopeUpper)) {
            markUndefined();
            return;
        }

        this.minAngle = Math.toDegrees(Math.atan(slopeLower));
        this.maxAngle = Math.toDegrees(Math.atan(slopeUpper));
        this.angle = Math.toDegrees(Math.atan(slopeEstimate));
        this.angleErrorMargin = (this.maxAngle - this.minAngle) / 180.0;
    }

    private void markUndefined() {
        this.angle = Double.POSITIVE_INFINITY;
        this.minAngle = Double.POSITIVE_INFINITY;
        this.maxAngle = Double.POSITIVE_INFINITY;
        this.angleErrorMargin = Double.POSITIVE_INFINITY;
    }

    /** True iff the leftmost sub-bucket extends before the sketch's segment start
     *  ({@code dpFrom[head] < windowId} in relative coords). Possible only when the
     *  cache populate path ran with {@code relaxedCacheReuse=true} and a boundary
     *  straddler sub-bucket was routed into this sketch's array (D2 shared partial,
     *  see {@code SketchUtils.addAggregatedDataPointToOverlappingSketches}). */
    private boolean hasLeftStraddler() {
        if (head == tail) return false;
        return dpFrom[head] < (double) windowId;
    }

    /** True iff the rightmost sub-bucket extends past the sketch's segment end. */
    private boolean hasRightStraddler() {
        if (head == tail) return false;
        double unitMs = originalAggregateInterval.toDuration().toMillis();
        double segRightRel = windowId + (to - from) / unitMs;
        return dpTo[tail - 1] > segRightRel;
    }

    /**
     * Straddler-aware bound (v2). Corner enumeration over (m_L, m_R) ∈ {0, count_L} ×
     * {0, count_R} per the proof in {@code docs/approxols-bound-proof-relaxed-alignment.md}
     * ("Computability" §1). For each existing straddler, run the basic LP twice — once
     * including the straddler at its full (count, sum) (m=count corner) and once
     * excluding it from the LP entirely (m=0 corner). Both corners are feasible
     * realisations of the unknown segment-resident sub-count; the union of their
     * slope envelopes brackets every interior realisation.
     *
     * <p>Returned: {min over corner lowers, estimate from include-all corner, max
     * over corner uppers}. Estimate fallback when the include-all corner degenerates:
     * midpoint of the union envelope.
     */
    private double[] cornerEnumBound(long N, boolean leftStrad, boolean rightStrad) {
        long countL = leftStrad ? dpCount[head] : 0L;
        long countR = rightStrad ? dpCount[tail - 1] : 0L;
        int leftChoices = leftStrad ? 2 : 1;
        int rightChoices = rightStrad ? 2 : 1;

        double slopeLow = Double.POSITIVE_INFINITY;
        double slopeHigh = Double.NEGATIVE_INFINITY;
        double slopeEstAll = Double.NaN;
        boolean anyFinite = false;

        for (int skipL = 0; skipL < leftChoices; skipL++) {
            for (int skipR = 0; skipR < rightChoices; skipR++) {
                int effStart = head + skipL;
                int effEnd = tail - skipR;
                if (effEnd - effStart < 2) continue;
                long effN = N - (skipL == 1 ? countL : 0L) - (skipR == 1 ? countR : 0L);
                if (effN < 2) continue;

                double[] b = weightedMeanBound(effStart, effEnd, effN);
                if (Double.isInfinite(b[0]) || Double.isInfinite(b[2])) continue;
                slopeLow = Math.min(slopeLow, b[0]);
                slopeHigh = Math.max(slopeHigh, b[2]);
                anyFinite = true;

                // Include-everything corner has the most data → most representative estimate.
                if (skipL == 0 && skipR == 0 && Double.isFinite(b[1])) {
                    slopeEstAll = b[1];
                }
            }
        }

        if (!anyFinite) {
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }
        if (Double.isNaN(slopeEstAll)) {
            slopeEstAll = (slopeLow + slopeHigh) * 0.5;
        }
        return new double[]{slopeLow, slopeEstAll, slopeHigh};
    }

    /**
     * Rigorous straddler-aware bound via the Möbius/decoupled-box construction
     * (part2-proof.tex §"The Möbius construction"). Replaces the cheap 2-corner
     * heuristic in {@link #cornerEnumBound}, which the proof shows is not a proof
     * (the slope-as-ratio in k need not be monotone between endpoints).
     *
     * <p>Sketch of the construction (full proof in part2-proof.tex):
     * <ol>
     *   <li><b>Möbius outer box for (X̂, D).</b> Both X̂(k_L, k_R) and D(k_L, k_R) are
     *       Möbius in each k_• (pole at -(N_int + k_other) < 0, outside the feasible
     *       range), so their joint range is achieved at the 4 corners of
     *       {0, c_L} × {0, c_R}.</li>
     *   <li><b>Outer box for s.</b> The feasible s-interval (U2)∩(U3) at each k is
     *       enclosed by [s⁻, s⁺] via O(1) evaluations of the two piecewise-linear faces
     *       at k ∈ {0, c_L} and the crossing kink.</li>
     *   <li><b>Outer box for Ŷ.</b> Interval-divide (C_int + [s_L⁻, s_L⁺] + [s_R⁻, s_R⁺])
     *       by [N_int, N_int + c_L + c_R].</li>
     *   <li><b>Decouple and corner-enumerate.</b> Treat (X̂, Ŷ, D) as independent of
     *       (k_L, s_L, k_R, s_R). Part I's bound is piecewise multilinear in this
     *       decoupled tuple (one parabola in X̂ from the Q_0 straddler term,
     *       handled by adding the vertex m_V as a candidate). Evaluate at corners +
     *       parabola vertices and union the slope sub-intervals.</li>
     * </ol>
     * Looseness vs. exact joint optimum: skip the {m_i, μ_i, (h_L+ℓ_L)/2}
     * piecewise-|·| / piecewise-d_V kinks — adding them only narrows the bound;
     * leaving them out preserves rigor (envelope only widens).
     *
     * <p>Cost: O(1) per straddler (≤ ~64 Part I evaluations total), independent of c_L, c_R.
     */
    private double[] mobiusStraddlerBound(boolean leftStrad, boolean rightStrad) {
        int idxL = head;
        int idxR = tail - 1;
        int internalStart = head + (leftStrad ? 1 : 0);
        int internalEnd = tail - (rightStrad ? 1 : 0);

        // Segment boundaries in relative coords. The virtual sub-bucket spans are
        // [segLeftRel, dpTo[head]] for L and [dpFrom[tail-1], segRightRel] for R —
        // i.e. only the in-segment portion, per part2-proof §"The virtual sub-bucket"
        // (NOT the full straddler bucket span).
        double segLeftRel = (double) windowId;
        double unitMs = originalAggregateInterval.toDuration().toMillis();
        double segRightRel = windowId + (to - from) / unitMs;

        // Straddler L data (0 when absent — guards multiplications below).
        long cL = leftStrad ? dpCount[idxL] : 0L;
        double GL = leftStrad ? dpSum[idxL] : 0.0;
        double ellL = leftStrad ? dpMin[idxL] : 0.0;
        double hL = leftStrad ? dpMax[idxL] : 0.0;
        double mVL = leftStrad ? (segLeftRel + dpTo[idxL]) * 0.5 : 0.0;
        double wVL = leftStrad ? (dpTo[idxL] - segLeftRel) : 0.0;

        long cR = rightStrad ? dpCount[idxR] : 0L;
        double GR = rightStrad ? dpSum[idxR] : 0.0;
        double ellR = rightStrad ? dpMin[idxR] : 0.0;
        double hR = rightStrad ? dpMax[idxR] : 0.0;
        double mVR = rightStrad ? (dpFrom[idxR] + segRightRel) * 0.5 : 0.0;
        double wVR = rightStrad ? (segRightRel - dpFrom[idxR]) : 0.0;

        // Internal aggregates: B_int = Σ c_i·m_i, H_int = Σ c_i·w_i/2,
        // C_int = Σ c_i·μ_i = Σ sum_i, N_int = Σ c_i.
        double bInt = 0.0, hInt = 0.0, cIntSum = 0.0;
        long nInt = 0L;
        for (int i = internalStart; i < internalEnd; i++) {
            int ci = dpCount[i];
            bInt += ci * dpMid[i];
            hInt += ci * dpHalfWidth[i];
            cIntSum += dpSum[i];
            nInt += ci;
        }

        // Möbius corners for X̂ and D over (k_L, k_R) ∈ {0, c_L} × {0, c_R}.
        // X̂(k_L, k_R) = (B_int + k_L·m_V_L + k_R·m_V_R) / (N_int + k_L + k_R)
        // D(k_L, k_R) = (H_int + k_L·w_V_L/2 + k_R·w_V_R/2) / (N_int + k_L + k_R)
        long[] kLChoices = leftStrad ? new long[]{0L, cL} : new long[]{0L};
        long[] kRChoices = rightStrad ? new long[]{0L, cR} : new long[]{0L};
        double xMin = Double.POSITIVE_INFINITY, xMax = Double.NEGATIVE_INFINITY;
        double dMin = Double.POSITIVE_INFINITY, dMax = Double.NEGATIVE_INFINITY;
        for (long kL : kLChoices) {
            for (long kR : kRChoices) {
                long denom = nInt + kL + kR;
                if (denom < 2) continue;
                double xhat = (bInt + kL * mVL + kR * mVR) / denom;
                double d = (hInt + kL * wVL * 0.5 + kR * wVR * 0.5) / denom;
                if (xhat < xMin) xMin = xhat;
                if (xhat > xMax) xMax = xhat;
                if (d < dMin) dMin = d;
                if (d > dMax) dMax = d;
            }
        }
        if (!Double.isFinite(xMin) || !Double.isFinite(xMax)) {
            // No feasible (k_L, k_R) with denom ≥ 2 — undefined.
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }

        // Outer box [s⁻, s⁺] for each straddler.
        double[] sLBox = leftStrad ? sOuterBox(cL, GL, ellL, hL) : new double[]{0.0, 0.0};
        double[] sRBox = rightStrad ? sOuterBox(cR, GR, ellR, hR) : new double[]{0.0, 0.0};

        // Outer box for Ŷ = (C_int + s_L + s_R) / (N_int + k_L + k_R).
        // Numerator interval × 1/denom interval (denom strictly > 0 if some data exists).
        long denomLo = Math.max(1L, nInt);
        long denomHi = nInt + cL + cR;
        if (denomHi < 2) {
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }
        double numLo = cIntSum + sLBox[0] + sRBox[0];
        double numHi = cIntSum + sLBox[1] + sRBox[1];
        double y1 = numLo / denomLo, y2 = numLo / denomHi;
        double y3 = numHi / denomLo, y4 = numHi / denomHi;
        double yMin = Math.min(Math.min(y1, y2), Math.min(y3, y4));
        double yMax = Math.max(Math.max(y1, y2), Math.max(y3, y4));

        // Candidate sets. X̂: corners + parabola vertex m_V (Q_0 straddler term is
        // convex in X̂; min at the vertex). Ŷ: corners + straddler-mean kinks
        // (|s − k·Ŷ| flips sign at Ŷ = s/k). D: corners only.
        double[] xCands = appendIfInside(new double[]{xMin, xMax},
                leftStrad ? mVL : Double.NaN, rightStrad ? mVR : Double.NaN, xMin, xMax);
        double meanLFull = (leftStrad && cL > 0) ? GL / (double) cL : Double.NaN;
        double meanRFull = (rightStrad && cR > 0) ? GR / (double) cR : Double.NaN;
        double[] yCands = appendIfInside(new double[]{yMin, yMax}, meanLFull, meanRFull, yMin, yMax);
        double[] dCands = new double[]{dMin, dMax};

        // (k, s) Möbius corners per straddler. At (0, 0) the straddler drops out;
        // at (c, G) it enters with its stored mean.
        double[][] lCorners = leftStrad
                ? new double[][]{{0.0, 0.0}, {(double) cL, GL}}
                : new double[][]{{0.0, 0.0}};
        double[][] rCorners = rightStrad
                ? new double[][]{{0.0, 0.0}, {(double) cR, GR}}
                : new double[][]{{0.0, 0.0}};

        double slopeLow = Double.POSITIVE_INFINITY;
        double slopeHigh = Double.NEGATIVE_INFINITY;
        double slopeEstAll = Double.NaN;
        boolean anyFinite = false;
        for (double[] lc : lCorners) {
            for (double[] rc : rCorners) {
                double kL = lc[0], sL = lc[1];
                double kR = rc[0], sR = rc[1];
                long denom = nInt + (long) kL + (long) kR;
                if (denom < 2) continue;
                for (double xCand : xCands) {
                    for (double yCand : yCands) {
                        for (double dCand : dCands) {
                            double[] b = evalDecoupledBound(internalStart, internalEnd,
                                    leftStrad, kL, sL, mVL, wVL, ellL, hL,
                                    rightStrad, kR, sR, mVR, wVR, ellR, hR,
                                    xCand, yCand, dCand);
                            if (Double.isInfinite(b[0]) || Double.isInfinite(b[2])) continue;
                            if (b[0] < slopeLow) slopeLow = b[0];
                            if (b[2] > slopeHigh) slopeHigh = b[2];
                            anyFinite = true;
                            // Pick the estimate from the "include both straddlers, decoupled means
                            // honest (closest-to-true X̂/Ŷ)" corner: kL=cL, kR=cR with X̂/Ŷ at
                            // the means computed from those k's.
                            if ((!leftStrad || kL == cL) && (!rightStrad || kR == cR)
                                    && Double.isFinite(b[1]) && Double.isNaN(slopeEstAll)) {
                                slopeEstAll = b[1];
                            }
                        }
                    }
                }
            }
        }

        if (!anyFinite) {
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }
        if (Double.isNaN(slopeEstAll)) {
            slopeEstAll = (slopeLow + slopeHigh) * 0.5;
        }
        return new double[]{slopeLow, slopeEstAll, slopeHigh};
    }

    /**
     * O(1) outer enclosure of the feasible s for a straddler bucket. The lower face
     * max(k·ℓ, G−(c−k)·h) and the upper face min(k·h, G−(c−k)·ℓ) are each piecewise
     * linear in k with at most one interior kink; evaluating at k ∈ {0, c} and the
     * kink gives the global min/max over k ∈ [0, c].
     */
    private static double[] sOuterBox(long c, double G, double ell, double h) {
        if (c <= 0) return new double[]{0.0, 0.0};
        double cD = (double) c;
        // Endpoints.
        double lo0 = Math.max(0.0, G - cD * h);
        double hi0 = Math.min(0.0, G - cD * ell);
        double loC = Math.max(cD * ell, G);
        double hiC = Math.min(cD * h, G);
        double sMin = Math.min(lo0, loC);
        double sMax = Math.max(hi0, hiC);
        // Lower face kink: k·ℓ = G − (c−k)·h ⇒ k = (G − c·h)/(ℓ − h). Skip if h = ℓ.
        if (h != ell) {
            double kKinkLo = (G - cD * h) / (ell - h);
            if (kKinkLo > 0.0 && kKinkLo < cD) {
                double v = kKinkLo * ell;
                if (v < sMin) sMin = v;
            }
            // Upper face kink: k·h = G − (c−k)·ℓ ⇒ k = (G − c·ℓ)/(h − ℓ).
            double kKinkHi = (G - cD * ell) / (h - ell);
            if (kKinkHi > 0.0 && kKinkHi < cD) {
                double v = kKinkHi * h;
                if (v > sMax) sMax = v;
            }
        }
        return new double[]{sMin, sMax};
    }

    /** Append c1 and c2 to base if they fall strictly inside [lo, hi]. NaN inputs are skipped. */
    private static double[] appendIfInside(double[] base, double c1, double c2, double lo, double hi) {
        int extra = 0;
        boolean use1 = !Double.isNaN(c1) && c1 > lo && c1 < hi;
        boolean use2 = !Double.isNaN(c2) && c2 > lo && c2 < hi && c2 != c1;
        if (use1) extra++;
        if (use2) extra++;
        if (extra == 0) return base;
        double[] out = Arrays.copyOf(base, base.length + extra);
        int j = base.length;
        if (use1) out[j++] = c1;
        if (use2) out[j] = c2;
        return out;
    }

    /**
     * Part I's slope envelope evaluated at decoupled (X̂, Ŷ, D) with the two
     * straddlers parameterized by (k, s, span). Counterpart to
     * {@link #weightedMeanBound} but with the global means/spread supplied externally
     * (per the Möbius construction's Step 3). Used as the leaf evaluator inside
     * {@link #mobiusStraddlerBound}.
     */
    private double[] evalDecoupledBound(int internalStart, int internalEnd,
                                        boolean leftStrad, double kL, double sL,
                                        double mVL, double wVL, double ellL, double hL,
                                        boolean rightStrad, double kR, double sR,
                                        double mVR, double wVR, double ellR, double hR,
                                        double xHat, double yHat, double dGlob) {
        double p0 = 0.0, q0 = 0.0, eP = 0.0, eDown = 0.0, eUpExtra = 0.0;

        // Internal contributions — exactly Part I's per-bucket terms.
        // Reads precomputed dpMid / dpHalfWidth / dpMean / dpYdev (bit-identical
        // to the inline recomputation they replaced).
        for (int i = internalStart; i < internalEnd; i++) {
            double halfWidth = dpHalfWidth[i];
            double width = halfWidth + halfWidth;
            double ci = dpCount[i];
            double devX = dpMid[i] - xHat;
            double devY = dpMean[i] - yHat;
            p0 += ci * devX * devY;
            q0 += ci * devX * devX;
            double ydev = dpYdev[i];
            eP += ci * Math.abs(devY) * halfWidth + ci * width * ydev;
            eDown += ci * Math.abs(devX) * width;
            double spread = halfWidth + dGlob;
            eUpExtra += ci * spread * spread + ci * halfWidth * halfWidth;
        }

        // Straddler L — virtual sub-bucket V with count k_L, sum s_L, span (m_V_L, w_V_L),
        // y-range [ℓ_L, h_L]. At k_L = 0 the straddler drops out entirely.
        if (leftStrad && kL > 0.0) {
            double halfWidth = wVL * 0.5;
            double devX = mVL - xHat;
            double meanV = sL / kL;
            double sMinusKY = sL - kL * yHat; // = k_L·(meanV − Ŷ)
            p0 += devX * sMinusKY;
            q0 += kL * devX * devX;
            double dV = Math.max(hL - meanV, meanV - ellL);
            eP += Math.abs(sMinusKY) * halfWidth + kL * wVL * dV;
            eDown += kL * Math.abs(devX) * wVL;
            double spread = halfWidth + dGlob;
            eUpExtra += kL * spread * spread + kL * halfWidth * halfWidth;
        }
        if (rightStrad && kR > 0.0) {
            double halfWidth = wVR * 0.5;
            double devX = mVR - xHat;
            double meanV = sR / kR;
            double sMinusKY = sR - kR * yHat;
            p0 += devX * sMinusKY;
            q0 += kR * devX * devX;
            double dV = Math.max(hR - meanV, meanV - ellR);
            eP += Math.abs(sMinusKY) * halfWidth + kR * wVR * dV;
            eDown += kR * Math.abs(devX) * wVR;
            double spread = halfWidth + dGlob;
            eUpExtra += kR * spread * spread + kR * halfWidth * halfWidth;
        }

        if (!(q0 > 0.0)) {
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }
        double slopeEstimate = p0 / q0;
        double sxxLo = q0 - eDown;
        double sxxHi = q0 + eDown + eUpExtra;
        double sxyLo = p0 - eP;
        double sxyHi = p0 + eP;
        if (!(sxxLo > 0.0)) {
            return new double[]{Double.NEGATIVE_INFINITY, slopeEstimate, Double.POSITIVE_INFINITY};
        }
        double r1 = sxyLo / sxxLo, r2 = sxyLo / sxxHi, r3 = sxyHi / sxxLo, r4 = sxyHi / sxxHi;
        double slopeLower = Math.min(Math.min(r1, r2), Math.min(r3, r4));
        double slopeUpper = Math.max(Math.max(r1, r2), Math.max(r3, r4));
        return new double[]{slopeLower, slopeEstimate, slopeUpper};
    }

    /**
     * Point estimate + rigorous envelope for the raw-sample OLS slope of the
     * sub-bucket range {@code [start, end)} (a contiguous slice of the parallel
     * arrays). Each sub-bucket contributes count_i raw points with x in
     * [a_i,b_i], y in [c_i,d_i] and known mean_i.
     *
     * <p>Called once from {@code calculateMinMaxAngle} when no straddlers are
     * present, and up to four times from {@code cornerEnumBound} as a sub-routine
     * (once per corner of (m_L, m_R) ∈ {0, count_L} × {0, count_R}).
     *
     * <p><b>Point estimate</b> — weighted OLS over the points (midpoint_i, mean_i)
     * with count weights. Exact in Ȳ and in the between-bucket structure.
     *
     * <p><b>Envelope</b> — vs. the true raw-sample OLS the estimate drops three
     * terms, each bounded per sub-bucket from geometry (width_i), count_i and
     * min/max/mean:
     * <ul>
     *   <li>midpoint vs. true x-centroid offset (|x̄_i − mid_i| ≤ width_i/2);</li>
     *   <li>within-bucket x-variance (0 ≤ W_xx_i ≤ count_i·width_i²/4) — only raises S_xx;</li>
     *   <li>within-bucket x/y covariance (|W_xy_i| ≤ width_i·count_i·max(d_i−mean_i, mean_i−c_i)).</li>
     * </ul>
     * S_xy and S_xx are enveloped from these and combined by interval division.
     * O(end−start) in sub-buckets; envelope collapses as sub-buckets get finer.
     *
     * <p>Returns {slopeLower, slopeEstimate, slopeUpper}. When the S_xx lower
     * bound is not positive the slope is unbounded and lower/upper are -inf/+inf.
     */
    private double[] weightedMeanBound(int start, int end, long N) {
        // Pass 1: weighted sums of mid, sum_y, halfWidth — needed to get X̄, Ȳ, ΔMax.
        // sum_y_i already incorporates the count_i weight (ΣY_bucket = count_i * mean_i),
        // so summing dpSum[i] directly gives the total raw-sample Y aggregate.
        // Reads dpMid / dpHalfWidth which were precomputed at add time and are
        // bit-identical to the inline `(dpFrom+dpTo)*0.5` / `(dpTo-dpFrom)*0.5`
        // these used to recompute — saves ~30% of pass-1 work on long composites.
        double sumNmid = 0.0, sumY = 0.0, sumNhalfWidth = 0.0;
        for (int i = start; i < end; i++) {
            double w = dpCount[i];
            sumNmid += w * dpMid[i];
            sumY += dpSum[i];
            sumNhalfWidth += w * dpHalfWidth[i];
        }
        double Xbar = sumNmid / N;
        double Ybar = sumY / N;
        // Rigorous bound on |X̄_true − X̄_est|.
        double deltaMax = sumNhalfWidth / N;

        // Pass 2: Sxx/Sxy + the |·|-based envelope terms (E_xy, E_xxDown, E_xxUpExtra).
        // Same arithmetic the previous inline version used; constants are pulled from
        // the dp* arrays (precomputed in addAggregatedDataPoint) so the inner loop is
        // pure read + arithmetic on already-derived values.
        double sxxEst = 0.0, sxyEst = 0.0;
        double eXy = 0.0, eXxDown = 0.0, eXxUpExtra = 0.0;
        for (int i = start; i < end; i++) {
            double halfWidth = dpHalfWidth[i];
            double width = halfWidth + halfWidth;
            double devX = dpMid[i] - Xbar;
            double w = dpCount[i];
            double devY = dpMean[i] - Ybar;
            sxxEst += w * devX * devX;
            sxyEst += w * devX * devY;

            double ydev = dpYdev[i];
            // E_xy: midpoint-offset term + within-bucket covariance term.
            eXy += w * Math.abs(devY) * halfWidth + w * width * ydev;
            // E_xx: the midpoint-offset term is the only one that can lower S_xx ...
            eXxDown += w * Math.abs(devX) * width;
            // ... the within-bucket x-spread terms can only raise it.
            double spread = halfWidth + deltaMax;
            eXxUpExtra += w * spread * spread + w * halfWidth * halfWidth;
        }

        if (!(sxxEst > 0)) {
            return new double[]{Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
        }
        double slopeEstimate = sxyEst / sxxEst;

        double sxxLo = sxxEst - eXxDown;
        double sxxHi = sxxEst + eXxDown + eXxUpExtra;
        double sxyLo = sxyEst - eXy;
        double sxyHi = sxyEst + eXy;

        if (!(sxxLo > 0)) {
            // The raw x's can collapse -> denominator can be 0 -> slope unbounded.
            return new double[]{Double.NEGATIVE_INFINITY, slopeEstimate, Double.POSITIVE_INFINITY};
        }

        // slope in [sxyLo,sxyHi] / [sxxLo,sxxHi], denominator strictly positive.
        double q1 = sxyLo / sxxLo, q2 = sxyLo / sxxHi, q3 = sxyHi / sxxLo, q4 = sxyHi / sxxHi;
        double slopeLower = Math.min(Math.min(q1, q2), Math.min(q3, q4));
        double slopeUpper = Math.max(Math.max(q1, q2), Math.max(q3, q4));
        return new double[]{slopeLower, slopeEstimate, slopeUpper};
    }

    public List<ReferenceDataPoint> getAllDataPoints() {
        int sz = tail - head;
        List<ReferenceDataPoint> out = new ArrayList<>(sz);
        for (int i = head; i < tail; i++) {
            out.add(new ReferenceDataPoint(dpFrom[i], dpTo[i], dpMin[i], dpMax[i], dpSum[i], dpCount[i]));
        }
        return out;
    }

    public void addDataPoint(DataPoint dp){
        throw new UnsupportedOperationException("This sketch does not support adding individual data points directly. Use addAggregatedDataPoint instead.");
    }

    static final class ReferenceDataPoint {
        private final double from;
        private final double to;
        private final double maxValue;
        private final double minValue;
        private final double sum;
        private final int count;

        public ReferenceDataPoint(double from, double to, double minValue, double maxValue, double sum, int count) {
            this.from = from;
            this.to = to;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.sum = sum;
            this.count = count;
        }

        public double getFrom() {
            return from;
        }

        public double getTo(){
            return to;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public double getMinValue() {
            return minValue;
        }

        public double getSum() {
            return sum;
        }

        public double getMean() {
            return count > 0 ? sum / count : Double.NaN;
        }

        public int getCount() {
            return count;
        }

        @Override
        public String toString() {
            return "ReferenceDataPoint{" +
                    "from=" + from +
                    ", to=" + to +
                    ", minValue=" + minValue +
                    ", maxValue=" + maxValue +
                    ", sum=" + sum +
                    ", count=" + count +
                    '}';
        }
    }

    @Override
    public String toString(){
        return "NonTimestampedSketch{" +
                "from=" + getFromDate() +
                ", to=" + getToDate() +
                '}';
    }
}
