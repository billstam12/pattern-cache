package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;

/** Only for non timestampe stats = agg. time series spans */
public class TimestampedSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(NonTimestampedSketch.class);

    private long from;
    private long to;

    // Store the stats from each interval
    private StatsAggregator statsAggregator = new StatsAggregator();
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;
    
    private double slope;

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // Track first and last data points for slope calculation
    private DataPoint firstDataPoint;
    private DataPoint lastDataPoint;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public TimestampedSketch(long from, long to, AggregationType aggregationType) {
        this.from = from;
        this.to = to;
        this.aggregationType = aggregationType;
    }

    /**
     * Adds an aggregated data point to this sketch, using the configured aggregation type.
     *
     * @param dp The aggregated data point to add
     */
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true; // Mark as having underlying data
        Stats stats = dp.getStats();
        if (stats.getCount() > 0) {
            // Track first and last data points for slope calculation
            if (firstDataPoint == null || dp.getTimestamp() < firstDataPoint.getTimestamp()) {
                firstDataPoint = new ImmutableDataPoint(dp.getStats().getFirstTimestamp(), dp.getStats().getFirstValue());
            }
            if (lastDataPoint == null || dp.getTimestamp() > lastDataPoint.getTimestamp()) {
                lastDataPoint = new ImmutableDataPoint(dp.getStats().getLastTimestamp(), dp.getStats().getLastValue());
            }
            
            statsAggregator.accept(dp);
        }
    }
    
    /**
     * Combines this sketch with another one, extending the time interval and updating stats.
     * The sketches must be consecutive (this.to == other.from).
     * 
     * @param other The sketch to combine with this one
     * @return This sketch after combination (for method chaining)
     * @throws IllegalArgumentException if the sketches are not consecutive
     */
    @Override
    public Sketch combine(Sketch other) {
        // Validate input
        if (other == null) {
            LOG.debug("Attempt to combine with null sketch, returning this sketch unchanged");
            return this;
        }
        
        if (other.isEmpty()) {
            LOG.debug("Attempt to combine with empty sketch, returning this sketch unchanged");
            return this;
        }

        if (!(other instanceof TimestampedSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        TimestampedSketch otherSketch = (TimestampedSketch) other;
        validateConsecutiveSketch(otherSketch);
        validateCompatibleAggregationTypes(otherSketch);

        // Calculate slope between consecutive sketches before combining stats
        computeSlopeBetweenConsecutiveSketches(this, otherSketch);
        // Update time interval and duration
        this.to = otherSketch.getTo();
        
        // Update first/last data points
        if (otherSketch.firstDataPoint != null) {
            if (this.firstDataPoint == null || otherSketch.firstDataPoint.getTimestamp() < this.firstDataPoint.getTimestamp()) {
                this.firstDataPoint = otherSketch.firstDataPoint;
            }
        }
        
        if (otherSketch.lastDataPoint != null) {
            if (this.lastDataPoint == null || otherSketch.lastDataPoint.getTimestamp()  > this.lastDataPoint.getTimestamp() ) {
                this.lastDataPoint = otherSketch.lastDataPoint;
            }
        }
        
        // Combine stats
        this.statsAggregator.combine(otherSketch.getStatsAggregator());
        
        return this;
    }
    
    /**
     * Validates that the other sketch is consecutive to this one
     * 
     * @param other The sketch to validate against this one
     * @throws IllegalArgumentException if sketches are not consecutive
     */
    private void validateConsecutiveSketch(Sketch other) {
        if (this.getTo() != other.getFrom()) {
            throw new IllegalArgumentException(
                String.format("Cannot combine non-consecutive sketches. Current sketch ends at %d but next sketch starts at %d", 
                              this.getTo(), other.getFrom())
            );
        }
    }
    
    /**
     * Validates that both sketches use compatible aggregation types
     * 
     * @param other The sketch to validate against this one
     */
    private void validateCompatibleAggregationTypes(Sketch other) {
        if (this.aggregationType != other.getAggregationType()) {
            LOG.warn("Combining sketches with different aggregation types: {} and {}", 
                this.aggregationType, other.getAggregationType());
        }
    }
   
    // Accessors and utility methods
    
    /**
     * Gets the aggregation type used by this sketch.
     *
     * @return The aggregation type
     */
    public AggregationType getAggregationType() {
        return aggregationType;
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
    public double getSlope() {
        return slope;
    }

    public Sketch clone() {
        TimestampedSketch sketch = new TimestampedSketch(this.from, this.to, this.aggregationType);
        sketch.statsAggregator = this.statsAggregator.clone();
        sketch.hasInitialized = this.hasInitialized;
        sketch.slope = this.slope;
        sketch.firstDataPoint = this.firstDataPoint;
        sketch.lastDataPoint = this.lastDataPoint;
        return sketch;
    }

    public StatsAggregator getStatsAggregator() {
        return statsAggregator;
    }

    public boolean isEmpty() {
        return statsAggregator.getCount() == 0;
    }

    public boolean hasInitialized() {
        return hasInitialized;
    }
    
    /**
     * Gets the first data point added to this sketch.
     * 
     * @return The first aggregated data point
     */
    public DataPoint getFirstAddedDataPoint() {
        return firstDataPoint;
    }
    
    /**
     * Gets the last data point added to this sketch.
     * 
     * @return The last aggregated data point
     */
    public DataPoint getLastAddedDataPoint() {
        return lastDataPoint;
    }
    
    /**
     * Calculates the slope between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeSlopeBetweenConsecutiveSketches(TimestampedSketch first, TimestampedSketch second) {
        DataPoint firstPoint = first.getReferencePointFromSketch();
        DataPoint secondPoint = second.getReferencePointFromSketch();
        LOG.debug("Calculating slope between sketches from {} to {}", firstPoint, secondPoint);
        if (firstPoint == null || secondPoint == null) {
            LOG.debug("Insufficient data points to calculate slope between sketches");
            this.slope = 0.0;
            return;
        }
        // Calculate value change
        double valueChange = secondPoint.getValue() - firstPoint.getValue();
        // Calculate time change
        long timeChange = secondPoint.getTimestamp()  - firstPoint.getTimestamp();

        double normalizedTimeChange = timeChange / (double)(second.getTo() - first.getFrom());

        double rawSlope = valueChange / normalizedTimeChange;

        this.slope = Math.atan(rawSlope) / Math.PI;

        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting slope to 0");
            this.slope = 0.0;
            return;
        }
        
        LOG.debug("Calculated slope between consecutive sketches {} and {} : {} (range: {} to {}), error margin: {}", 
                first.getFromDate(), second.getFromDate(), this.slope);
    }
    
    /**
     * Gets the appropriate reference point from a sketch based on aggregation type.
     * For the first sketch in a sequence, we typically want the last data point.
     * For the second sketch in a sequence, we typically want the first data point.
     *
     * @param sketch The sketch to get the reference point from
     * @param isFirstInSequence Whether this is the first sketch in the sequence
     * @return The reference data point based on aggregation type
     */
    private DataPoint getReferencePointFromSketch() {
        if (isEmpty()) {
            return null;
        }
        
        switch (aggregationType) {
            case FIRST_VALUE:
                return getFirstAddedDataPoint();
            case LAST_VALUE:
                return getLastAddedDataPoint();
            // case MIN_VALUE:
            // case MAX_VALUE:
            //     // For min/max, use the data point that would be selected by the aggregation type
            //     if (isFirstInSequence) {
            //         return sketch.aggregationType == AggregationType.MIN_VALUE ? 
            //                sketch.getStatsAggregator().getMinDataPoint() :
            //                sketch.getStatsAggregator().getMaxDataPoint();
            //     } else {
            //         return sketch.aggregationType == AggregationType.MIN_VALUE ?
            //                sketch.getStatsAggregator().getMinDataPoint() :
            //                sketch.getStatsAggregator().getMaxDataPoint();
            //     }
            default:
                // Default to first/last based on position in sequence
                return getLastAddedDataPoint();
        }
    }
    
}
