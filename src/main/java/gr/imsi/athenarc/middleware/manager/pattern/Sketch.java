package gr.imsi.athenarc.middleware.manager.pattern;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;

import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

public class Sketch implements TimeInterval {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    /**
     * Defines the different aggregation types that can be used when creating sketches
     */
    public enum AggregationType {
        LAST_VALUE,   // Use the last value from each interval
        FIRST_VALUE,  // Use the first value from each interval
        MIN_VALUE,    // Use the minimum value from each interval
        MAX_VALUE,    // Use the maximum value from each interval
        AVERAGE_VALUE // Use the average value from each interval
    }

    private long from;
    private long to;

    // Store the values from each interval
    private List<Double> values = new ArrayList<>();

    // used to know if a sketch is empty because it hasnt been initialized
    private boolean hasZeroCountPoint = false;
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;

    /**
     * Creates a new sketch with the default aggregation type (LAST_VALUE).
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     */
    public Sketch(long from, long to) {
        this(from, to, AggregationType.LAST_VALUE);
    }
    
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
        Stats stats = dp.getStats();
        if (stats.getCount() > 0) {
            // We store the value based on the configured aggregation type
            values = new ArrayList<>();
            values.add(getValueByAggregationType(stats));
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
            case AVERAGE_VALUE:
                return stats.getAverageValue();
            case LAST_VALUE:
            default:
                return stats.getLastValue();
        }
    }
    
    public void combine(Sketch sketch) {
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
        // Preserve the order of values when combining sketches
        values.addAll(sketch.getValues());
        LOG.debug("Combined sketch {}", values);
    }
    
    /**
     * Gets the aggregation type used by this sketch.
     *
     * @return The aggregation type
     */
    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public double computeSlope() {        
        // Check if there are enough data points to calculate slope
        if (this.getDuration() < 1) {
            return 0;
        }
                
        // Calculate slope using first and last data points in the composite sketch
        double firstValue = values.get(0);
        double lastValue = values.get(values.size() - 1);
        double valueChange = lastValue - firstValue;
        
        // Calculate slope over the actual time duration, not just the number of values
        double slope = valueChange / getDuration();
        
        // Normalize the slope using arctangent
        double normalizedSlope = Math.atan(slope) / Math.PI; // Maps to range [-0.5,0.5]

        return normalizedSlope;
    }

    public int getDuration(){
        return values.size() - 1;
    }

    @Override
    public long getFrom() {
        return from;
    }
    @Override
    public long getTo() {
        return to;
    }

    public List<Double> getValues() {
        return values;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Sketch from ").append(from).append(" to ").append(to);
        sb.append(" duration ").append(getDuration());
        sb.append(" aggregationType ").append(aggregationType);
        if (values.size() >= 2) {
            sb.append(" slope: ").append(computeSlope());
            sb.append(" (first: ").append(values.get(0));
            sb.append(", last: ").append(values.get(values.size() - 1)).append(")");
        } else {
            sb.append(" (insufficient values for slope)");
        }
        return sb.toString();
    }

    public Sketch clone(){
        Sketch sketch = new Sketch(this.from, this.to, this.aggregationType);
        sketch.values = new ArrayList<>(values);
        sketch.hasZeroCountPoint = this.hasZeroCountPoint;
        return sketch;
    }

    /**
     * Marks this sketch as having data points with count=0,
     * indicating there's no data for this interval in the database.
     */
    public void markAsZeroCount() {
        this.hasZeroCountPoint = true;
    }

    /**
     * Checks if this sketch has been marked as having no data in the database.
     * 
     * @return true if this sketch contains data points with count=0
     */
    public boolean hasZeroCountPoint() {
        return hasZeroCountPoint;
    }

    /**
     * Returns the first value of this sketch if available.
     * 
     * @return the first value or null if no values are present
     */
    public Double getFirstValue() {
        return values.isEmpty() ? null : values.get(0);
    }

    /**
     * Returns the last value of this sketch if available.
     * 
     * @return the last value or null if no values are present
     */
    public Double getLastValue() {
        return values.isEmpty() ? null : values.get(values.size() - 1);
    }
}
