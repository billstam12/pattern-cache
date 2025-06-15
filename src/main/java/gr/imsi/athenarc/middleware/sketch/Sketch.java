package gr.imsi.athenarc.middleware.sketch;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import java.util.Optional;

/**
 * Interface for sketches that represent time series data segments
 * with calculated statistics and angles between segments.
 */
public interface Sketch extends TimeInterval {
    
    /**
     * Adds an aggregated data point to this sketch
     * 
     * @param dataPoint The data point to add
     */
    void addAggregatedDataPoint(AggregatedDataPoint dataPoint);
    
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
     * Gets the start timestamp of this sketch
     * 
     * @return Start timestamp in epoch milliseconds
     */
    long getFrom();
    
    /**
     * Gets the end timestamp of this sketch
     * 
     * @return End timestamp in epoch milliseconds
     */
    long getTo();
    
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
     * Gets the aggregation type used by this sketch
     * 
     * @return The aggregation type
     */
    AggregationType getAggregationType();
    
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
     * Gets the original time interval used in this sketch
     * 
     * @return The aggregate interval
     */
    Optional<AggregateInterval> getOriginalAggregateInterval();
}
