package gr.imsi.athenarc.middleware.sketch;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import java.util.Optional;

/**
 * Interface for sketches that represent time series data segments
 * with calculated statistics and angles between segments.
 */
public interface Sketch extends TimeInterval {
    
    public void addDataPoint(DataPoint dp);

    /**
     * Adds an aggregated data point to this sketch
     *
     * @param dataPoint The data point to add
     */
    void addAggregatedDataPoint(AggregatedDataPoint dataPoint);

    /**
     * Pre-size internal storage to accommodate {@code expectedTotalSubBuckets}
     * sub-bucket points (active count, ignoring head dead-space). Pure perf hint —
     * skips the geometric-doubling resize cascade in {@link #addAggregatedDataPoint}
     * / {@link #combine}. Calling with a low estimate is harmless (subsequent
     * resizes still work); over-estimating wastes a bounded amount of memory.
     * Default is a no-op for sketch types that don't expose backing arrays.
     */
    default void reserveCapacity(int expectedTotalSubBuckets) {
        // no-op
    }
    
    /**
     * Checks if this sketch can be combined with another sketch
     * 
     * @param other The sketch to check compatibility with
     * @return true if sketches can be combined, false otherwise
     */
    default boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            return false;
        }
        
        // Check temporal continuity
        return this.getTo() == other.getFrom();
    }
    
    /**
     * Combines this sketch with another one
     * 
     * @param other The sketch to combine with
     * @return The combined sketch (typically this instance)
     * @throws IllegalArgumentException if sketches cannot be combined
     */
    Sketch combine(Sketch other);
    
    /**
     * Creates a clone of this sketch
     * 
     * @return A new sketch with the same properties
     */
    Sketch clone();
    
    /**
     * Gets the angle (slope) calculated for this sketch
     * 
     * @return The angle in degrees
     */
    double getAngle();
    
    /**
     * Gets the minimum possible angle value considering error bounds
     * 
     * @return Minimum angle in degrees
     */
    default double getMinAngle() {
        return getAngle(); // Default implementation returns the exact angle
    }
    
    /**
     * Gets the maximum possible angle value considering error bounds
     * 
     * @return Maximum angle in degrees
     */
    default double getMaxAngle() {
        return getAngle(); // Default implementation returns the exact angle
    }
    
    /**
     * Gets the error margin in angle calculation
     * 
     * @return Error margin as a value between 0 and 1
     */
    default double getAngleErrorMargin() {
        return 0; // Default implementation has no error
    }
    
    /**
     * Checks if sketch has been initialized with data
     * 
     * @return true if initialized, false otherwise
     */
    boolean hasInitialized();
    
    /**
     * Checks if this sketch contains any data
     * 
     * @return true if empty, false otherwise
     */
    boolean isEmpty();
    
    /**
     * Checks if this sketch's angle matches the value filter
     *
     * @param filter The filter to check against
     * @return true if the angle matches the filter, false otherwise
     */
    default boolean matches(ValueFilter filter) {
        if (filter == null || filter.isValueAny()) {
            return true;
        }

        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return getAngle() >= low && getAngle() <= high;
    }

    /**
     * Relaxed filter check: the sketch's angle bound [minAngle, maxAngle] intersects
     * the filter range. True iff the true angle *could* lie inside the filter given
     * current uncertainty. Used by the cached pattern path's relaxed NFA pass to
     * surface ambiguous candidates that a midpoint check would miss.
     */
    default boolean boundIntersects(ValueFilter filter) {
        if (filter == null || filter.isValueAny()) {
            return true;
        }
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return getMaxAngle() >= low && getMinAngle() <= high;
    }

    /**
     * Confident filter check: the sketch's full angle bound lies inside the filter.
     * True iff the true angle is provably in the filter regardless of approximation
     * uncertainty. Distinguishes confident matches from ambiguous ones post-NFA.
     */
    default boolean boundContainedIn(ValueFilter filter) {
        if (filter == null || filter.isValueAny()) {
            return true;
        }
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return getMinAngle() >= low && getMaxAngle() <= high;
    }

    /**
     * Fraction of this sketch's angle bound [minAngle, maxAngle] that lies outside
     * {@code filter}, in [0, 1]. 0 = bound fully contained (would classify confident);
     * 1 = bound entirely outside. Returns 0 for an Any/null filter or a degenerate
     * (non-positive / non-finite) bound width — those carry no overshoot signal.
     */
    default double boundOvershoot(ValueFilter filter) {
        if (filter == null || filter.isValueAny()) {
            return 0.0;
        }
        double lo = getMinAngle();
        double hi = getMaxAngle();
        double width = hi - lo;
        if (!(width > 0) || Double.isNaN(width) || Double.isInfinite(width)) {
            return 0.0;
        }
        double outside = Math.max(0.0, filter.getMinDegree() - lo)
                + Math.max(0.0, hi - filter.getMaxDegree());
        return Math.min(1.0, outside / width);
    }

    /**
     * Gets the original time interval used in this sketch (before combining)
     *
     * @return The aggregate interval
     */
    Optional<AggregateInterval> getOriginalAggregateInterval();

    /**
     * Opt-in capability for the BFS sliding-window optimization. Returning true
     * commits this sketch type to a valid pair of {@link #dataPointCount()} and
     * {@link #removeFrontDataPoints(int, long)} implementations.
     */
    default boolean supportsSliding() {
        return false;
    }

    /** Number of underlying chunks contributing to this composite — required if {@link #supportsSliding()}. */
    default int dataPointCount() {
        throw new UnsupportedOperationException(getClass().getName() + " does not support sliding");
    }

    /** Drop the {@code n} oldest chunks from the front and reset {@code from} — required if {@link #supportsSliding()}. */
    default void removeFrontDataPoints(int n, long newFrom) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support sliding");
    }

    /**
     * Opt-in capability for the relaxed-alignment cache reuse path: returning true
     * commits this sketch type to a correct {@link #addAggregatedDataPoint} under
     * the D2 shared-partial discipline, where the same boundary sub-bucket may be
     * added to two adjacent sketches and the seam dedupe in {@code combine()}
     * keeps the chained sub-bucket list gap-free overlap-free. Sketch types that
     * accumulate sub-bucket stats into a single running aggregator (e.g.
     * {@code OLSSketch}'s slope sums) MUST leave this {@code false} — adding the
     * same physical sub-bucket twice would double-count its contribution.
     */
    default boolean supportsRelaxedAlignment() {
        return false;
    }
}
