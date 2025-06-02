package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.Sketch;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** Only for non timestamped aggregate stats = agg. time series spans */
public class M4Sketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(M4Sketch.class);

    private long from;
    private long to;

    // Store the stats from each interval
    private StatsAggregator statsAggregator = new StatsAggregator();
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;
    
    private double angle;

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // The interval this sketch represents. If it is combined it is the interval of its sub-sketches that created it.
    // Used in angle calculation.
    private AggregateInterval originalAggregateInterval; 

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public M4Sketch(long from, long to, AggregationType aggregationType) {
        this.from = from;
        this.to = to;
        this.aggregationType = aggregationType;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
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

        if (!(other instanceof M4Sketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        M4Sketch otherSketch = (M4Sketch) other;
        validateConsecutiveSketch(otherSketch);
        validateCompatibleAggregationTypes(otherSketch);

        // Calculate angle between consecutive sketches before combining stats
        computeAngleBetweenConsecutiveSketches(this, otherSketch);
        // Update time interval and duration
        this.to = otherSketch.getTo();        
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
    public double getAngle() {
        return angle;
    }
    
    public Sketch clone() {
        M4Sketch sketch = new M4Sketch(this.from, this.to, this.aggregationType);
        sketch.statsAggregator = this.statsAggregator.clone();
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
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
     * Computes the slope of a composite sketch against the ValueFilter of a segment.
     * Returns true if the slope is within the filter's range.
     */
    public boolean matches(ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        boolean match = angle >= low && angle <= high;
        return match;
    }

    /**
     * Calculates the angle between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeAngleBetweenConsecutiveSketches(M4Sketch first, M4Sketch second) {
        DataPoint firstPoint = first.getReferencePointFromSketch();
        DataPoint secondPoint = second.getReferencePointFromSketch();
        LOG.debug("Calculating angle between sketches from {} to {}", firstPoint, secondPoint);
        if (firstPoint == null || secondPoint == null) {
            LOG.debug("Insufficient data points to calculate angle between sketches");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        // Calculate value change
        double valueChange = secondPoint.getValue() - firstPoint.getValue();
        // Calculate time change
        long timeChange = (second.getFrom() - first.getFrom()) / (originalAggregateInterval.toDuration().toMillis());

        // Check for zero time difference before division
        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting angle to 0");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        
        double slope = valueChange / timeChange;
        double radians = Math.atan(slope);
        this.angle = Math.toDegrees(radians);
        
        LOG.debug("Calculated angle between consecutive sketches {} and {} : {}", 
                first, second, this.angle);
    }
    
    private DataPoint getReferencePointFromSketch() {
        if (isEmpty()) {
            return null;
        }
        
        switch (aggregationType) {
            case FIRST_VALUE:
                return statsAggregator.getLastDataPoint();
            case LAST_VALUE:
                return statsAggregator.getLastDataPoint();
            case MIN_VALUE:
                return statsAggregator.getMinDataPoint();
            case MAX_VALUE:
                return statsAggregator.getMaxDataPoint();
            default:
                throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
        }
    }
    
    @Override
    public String toString(){
        return "NonTimestampedSketch{" +
                "from=" + getFromDate() +
                ", to=" + getToDate() +
                ", referencePoint=" + getReferencePointFromSketch() +
                '}';
    }
}
