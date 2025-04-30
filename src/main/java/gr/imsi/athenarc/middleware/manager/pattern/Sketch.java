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
     * @param aggregationType The type of aggregation to use when adding data points
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
     * Gets the appropriate value from stats based on the configured aggregation type.
     *
     * @param stats The stats to get the value from
     * @return The value according to the configured aggregation type
     */
    private double getValueByAggregationType(Stats stats) {
        switch (aggregationType) {
            case FIRST_VALUE:
                return stats.getFirstValue();
            case MIN_VALUE:
                return stats.getMinValue();
            case MAX_VALUE:
                return stats.getMaxValue();
            case LAST_VALUE:
            default:
                return stats.getLastValue();
        }
    }
    
    public void combine(Sketch sketch) {
        if(sketch.getStatsAggregator().getCount() == 0) {
            return; // No data to combine
        }
        
        if (this.getTo() != sketch.getFrom()) {
            throw new IllegalArgumentException("Cannot combine sketches that are not consecutive. " +
                "Current sketch ends at " + this.getTo() + " but next sketch starts at " + sketch.getFrom());
        }
        
        // Ensure both sketches use the same aggregation type
        if (this.aggregationType != sketch.getAggregationType()) {
            LOG.warn("Combining sketches with different aggregation types: {} and {}", 
                this.aggregationType, sketch.getAggregationType());
        }
        
        this.to = sketch.getTo();
        this.duration += 1;
        computeSlope(sketch.getStatsAggregator()); // compute the slope before combining
        this.statsAggregator.combine(sketch.getStatsAggregator());
    }
    
    /**
     * Gets the aggregation type used by this sketch.
     *
     * @return The aggregation type
     */
    public AggregationType getAggregationType() {
        return aggregationType;
    }

    private void computeSlope(Stats otherStats) {        
        // Check if there are enough sketches to calculate slope
        if (this.getDuration() < 1) {
            return;
        }
                 
        // Calculate slope using first and last data points in the composite sketch
        double firstValue = getValueByAggregationType(statsAggregator);
        double lastValue = getValueByAggregationType(otherStats);
        double valueChange = lastValue - firstValue;
        
        // Calculate slope over the actual time duration, not just the number of values
        double slope = valueChange / getDuration();
        
        // Normalize the slope using arctangent
        double normalizedSlope = Math.atan(slope) / Math.PI; // Maps to range [-0.5,0.5]

        this.slope = normalizedSlope;
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
