package gr.imsi.athenarc.middleware.sketch;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.StatsUtils;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** Only for non timestamped aggregate stats = agg. time series spans */
public class SingleValueSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(SingleValueSketch.class);

    private long from;
    private long to;

    private double angle;

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // The interval this sketch represents. If it is combined it is the interval of its sub-sketches that created it.
    // Used in angle calculation.
    private AggregateInterval originalAggregateInterval; 

    private DataPoint dataPoint;

    private AggregationType aggregationType;
    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public SingleValueSketch(long from, long to, AggregationType aggregationType) {
        this.from = from;
        this.to = to;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
        this.aggregationType = aggregationType;
    }

    /**
     * Adds an aggregated data point to this sketch, using the configured aggregation type.
     *
     * @param dp The aggregated data point to add
     */
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        this.hasInitialized = true;
        if(dp == null) {
            throw new IllegalArgumentException("Cannot add null aggregated data point to sketch.");
        }
        if(this.dataPoint != null){
            throw new IllegalStateException("Cannot add aggregated data point to a sketch that already has one.");
        }
        long firstTimestamp = StatsUtils.getFirstTimestampSafely(dp.getStats(), dp.getFrom());
        long lastTimestamp = StatsUtils.getLastTimestampSafely(dp.getStats(), dp.getTo());
        long maxTimestamp = StatsUtils.getMaxTimestampSafely(dp.getStats(), (dp.getFrom() + dp.getTo()) / 2);
        long minTimestamp = StatsUtils.getMinTimestampSafely(dp.getStats(), (dp.getFrom() + dp.getTo()) / 2);
        switch(aggregationType) {
            case FIRST_VALUE:
                this.dataPoint = new ImmutableDataPoint(firstTimestamp, dp.getStats().getFirstValue());
                break;
            case LAST_VALUE:
                this.dataPoint = new ImmutableDataPoint(lastTimestamp, dp.getStats().getLastValue());
                break;
            case MIN_VALUE:
                this.dataPoint = new ImmutableDataPoint(minTimestamp, dp.getStats().getMinValue());
                break;
            case MAX_VALUE:
                this.dataPoint = new ImmutableDataPoint(maxTimestamp, dp.getStats().getMaxValue());
                break;
            default:
                LOG.error("Unknown aggregation type: {}", aggregationType);
                throw new IllegalStateException("Unknown aggregation type: " + aggregationType);
        }
        LOG.debug("Added aggregated data point to sketch: {}", this.dataPoint);
    }
    
    /**
     * Checks if this sketch can be combined with another sketch.
     * 
     * @param other The sketch to check compatibility with
     * @return true if sketches can be combined, false otherwise
     */
    public boolean canCombineWith(Sketch other) {
        if (other == null ) {
            LOG.debug("Cannot combine with null sketch");
            return false;
        }

        if (other.isEmpty()) {
            LOG.debug("Cannot combine with empty sketch");
            return false;
        }
        
        if (!(other instanceof SingleValueSketch)) {
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
     */
    @Override
    public Sketch combine(Sketch other) {
        // Validate input
        if (!canCombineWith(other)) {
            LOG.debug("Cannot combine incompatible sketches");
            return this;
        }
        
        SingleValueSketch otherSketch = (SingleValueSketch) other;
        
        // Calculate angle between consecutive sketches before combining stats
        computeAngleBetweenConsecutiveSketches(this, otherSketch);
        
        // Update time interval and duration
        this.to = otherSketch.getTo();        
        this.dataPoint = otherSketch.dataPoint; 
        return this;
    }
    
    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
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
        return angle;
    }
    
    public Sketch clone() {
        SingleValueSketch sketch = new SingleValueSketch(this.from, this.to, this.aggregationType);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        sketch.dataPoint = this.dataPoint != null ? new ImmutableDataPoint(this.dataPoint.getTimestamp(), this.dataPoint.getValue()) : null;
        return sketch;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public boolean isEmpty() {
        return dataPoint == null;
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

    public void addDataPoint(DataPoint dp){
        throw new UnsupportedOperationException("This sketch does not support adding individual data points directly. Use addAggregatedDataPoint instead.");
    }

    /**
     * Calculates the angle between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeAngleBetweenConsecutiveSketches(SingleValueSketch first, SingleValueSketch second) {
        DataPoint firstPoint = first.getReferencePointFromSketch();
        DataPoint secondPoint = second.getReferencePointFromSketch();
        
        if (firstPoint == null || secondPoint == null) {
            LOG.warn("Insufficient data points to calculate angle between sketches");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        // Calculate value change
        double valueChange = secondPoint.getValue() - firstPoint.getValue();
        
        // Calculate time change
        long timeChange = (secondPoint.getTimestamp() - firstPoint.getTimestamp()) / (originalAggregateInterval.toDuration().toMillis());

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
        if (dataPoint == null) {
            LOG.warn("No aggregated data point available in sketch to get reference point");
            return null;
        }
        return dataPoint;
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
