package gr.imsi.athenarc.middleware.manager.pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

public class Sketch implements TimeInterval {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private long from;
    private long to;

    // Store the values from each interval
    private StatsAggregator statsAggregator = new StatsAggregator();
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;
    
    private double slope;

    private int duration = 1;

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public Sketch(long from, long to, AggregationType aggregationType) {
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
            statsAggregator.accept(dp);
        }
    }
    
    /**
     * Gets the representative value of this sketch based on the configured aggregation type.
     *
     * @return The representative value of this sketch
     */
    public double getRepresentativeValue() {
        switch (aggregationType) {
            case FIRST_VALUE:
                return statsAggregator.getFirstValue();
            case MIN_VALUE:
                return statsAggregator.getMinValue();
            case MAX_VALUE:
                return statsAggregator.getMaxValue();
            case LAST_VALUE:
            default:
                return statsAggregator.getLastValue();
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
    public Sketch combine(Sketch other) {
        if(other == null || other.getStatsAggregator().getCount() == 0) {
            return this; // No data to combine
        }
        
        if (this.getTo() != other.getFrom()) {
            throw new IllegalArgumentException("Cannot combine sketches that are not consecutive. " +
                "Current sketch ends at " + this.getTo() + " but next sketch starts at " + other.getFrom());
        }
        
        // Ensure both sketches use the same aggregation type
        if (this.aggregationType != other.getAggregationType()) {
            LOG.warn("Combining sketches with different aggregation types: {} and {}", 
                this.aggregationType, other.getAggregationType());
        }
        
        // Calculate slope before combining stats
        calculateSlope(this.getRepresentativeValue(), other.getRepresentativeValue());
        
        // Update time interval and duration
        this.to = other.getTo();
        this.duration += other.getDuration();
        
        // Combine stats
        this.statsAggregator.combine(other.getStatsAggregator());
        
        return this; // Enable method chaining
    }
    
    /**
     * Calculate the slope between this sketch and another value
     * 
     * @param startValue The start value (typically from this sketch)
     * @param endValue The end value (typically from the next sketch)
     */
    private void calculateSlope(double startValue, double endValue) {
        // Calculate slope over the actual time duration
        double valueChange = endValue - startValue;
        double timeDifference = getDuration();
        
        if (timeDifference > 0) {
            double rawSlope = valueChange / timeDifference;
            // Normalize to range [-0.5, 0.5] using arctangent
            this.slope = Math.atan(rawSlope) / Math.PI;
        }
    }
    
    /**
     * Gets the aggregation type used by this sketch.
     *
     * @return The aggregation type
     */
    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public int getDuration(){
        return duration;
    }

    @Override
    public long getFrom() {
        return from;
    }
    @Override
    public long getTo() {
        return to;
    }

    public double getSlope() {
        return slope;
    }

    public Sketch clone(){
        Sketch sketch = new Sketch(this.from, this.to, this.aggregationType);
        sketch.statsAggregator = this.statsAggregator.clone();
        sketch.hasInitialized = this.hasInitialized;
        return sketch;
    }

    public StatsAggregator getStatsAggregator() {
        return statsAggregator;
    }

    public boolean isEmpty(){
        return statsAggregator.getCount() == 0;
    }

    public boolean hasInitialized() {
        return hasInitialized;
    }
}
