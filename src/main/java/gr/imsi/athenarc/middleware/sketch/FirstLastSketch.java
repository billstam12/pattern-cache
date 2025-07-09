package gr.imsi.athenarc.middleware.sketch;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.SlopeStatsAggregator;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** Only for non timestamped aggregate stats = agg. time series spans */
public class FirstLastSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(FirstLastSketch.class);

    private long from;
    private long to;

    // Store the stats from each interval
    private SlopeStatsAggregator slopeStatsAggregator = new SlopeStatsAggregator();
        
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
    public FirstLastSketch(long from, long to) {
        this.from = from;
        this.to = to;
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
            slopeStatsAggregator.accept(dp);
        }
    }
    
    /**
     * Checks if this sketch can be combined with another sketch.
     * 
     * @param other The sketch to check compatibility with
     * @return true if sketches can be combined, false otherwise
     */
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            LOG.debug("Cannot combine with null or empty sketch");
            return false;
        }
        
        if (!(other instanceof FirstLastSketch)) {
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
        
        FirstLastSketch otherSketch = (FirstLastSketch) other;
                
        // Update time interval and duration
        this.to = otherSketch.getTo();        
        
        // Combine stats
        this.slopeStatsAggregator.combine(otherSketch.getSlopeStatsAggregator());
        
        computeAngle();
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
        FirstLastSketch sketch = new FirstLastSketch(this.from, this.to);
        sketch.slopeStatsAggregator = this.getSlopeStatsAggregator().clone();
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        return sketch;
    }

    public SlopeStatsAggregator getSlopeStatsAggregator() {
        return slopeStatsAggregator;
    }

    public boolean isEmpty() {
        return slopeStatsAggregator.getCount() == 0;
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

    
    private void computeAngle() {
        DataPoint firstDataPoint = getSlopeStatsAggregator().getFirstDataPoint();
        DataPoint lastDataPoint = getSlopeStatsAggregator().getLastDataPoint();
        
        LOG.debug("Calculating angle between sketches from {} to {}", firstDataPoint, lastDataPoint);
        
        if (firstDataPoint == null || lastDataPoint == null) {
            LOG.warn("Insufficient data points to calculate angle between sketches");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        // Calculate value change
        double valueChange = lastDataPoint.getValue() - firstDataPoint.getValue();
        
        // Calculate time change
        long timeChange = (lastDataPoint.getTimestamp() - firstDataPoint.getTimestamp()) / (originalAggregateInterval.toDuration().toMillis());

        // Check for zero time difference before division
        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting angle to 0");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        
        double slope = valueChange / timeChange;
        double radians = Math.atan(slope);
        this.angle = Math.toDegrees(radians);
        
        LOG.debug("Calculated angle between consecutive sketches {} and {} : {}", from, to, this.angle);
    }
    
    
    @Override
    public String toString(){
        return "NonTimestampedSketch{" +
                "from=" + getFromDate() +
                ", to=" + getToDate() +
                '}';
    }
}
