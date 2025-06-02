package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.Sketch;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.SlopeStats;
import gr.imsi.athenarc.middleware.domain.SlopeStatsAggregator;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** 
 * A sketch that uses Ordinary Least Squares regression to compute the slope (angle) 
 * of a time series segment.
 */
public class OLSSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(OLSSketch.class);

    private long from;
    private long to;
    
    private double angle;

    // used for fetching data from the db
    private boolean hasInitialized = false;
    
    // The interval this sketch represents
    private AggregateInterval originalAggregateInterval;
    
    private SlopeStatsAggregator slopeStatsAggregator = new SlopeStatsAggregator();

    private AggregationType aggregationType = AggregationType.OLS;

    /**
     * Creates a new OLS sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public OLSSketch(long from, long to, AggregationType aggregationType) {
        this.from = from;
        this.to = to;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
        this.aggregationType = aggregationType;
        // if(!(aggregationType == AggregationType.OLS)) {
        //     throw new IllegalArgumentException("OLSSketch can only be used with OLS aggregation type");
        // }
    }

    /**
     * Adds an aggregated data point to this sketch, using the configured aggregation type.
     *
     * @param dp The aggregated data point to add
     */
    @Override
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true; // Mark as having underlying data
        
        SlopeStats slopeStats = (SlopeStats) dp.getStats();
        slopeStatsAggregator.accept(slopeStats);
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

        if (!(other instanceof OLSSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        OLSSketch otherSketch = (OLSSketch) other;
        validateConsecutiveSketch(otherSketch);
        
        // Update time interval
        this.to = otherSketch.getTo();
        
        slopeStatsAggregator.combine(otherSketch.getSlopeStatsAggregator());
        calculateAngle(); // update angle

        return this;
    }
    
    /**
     * Validates that the other sketch is consecutive to this one
     * 
     * @param other The sketch to validate against this one
     * @throws IllegalArgumentException if sketches are not consecutive
     */
    private void validateConsecutiveSketch(OLSSketch other) {
        if (this.getTo() != other.getFrom()) {
            throw new IllegalArgumentException(
                String.format("Cannot combine non-consecutive sketches. Current sketch ends at %d but next sketch starts at %d", 
                              this.getTo(), other.getFrom())
            );
        }
    }
    
    /**
     * Calculate the slope (angle) using Ordinary Least Squares regression.
     */
    private void calculateAngle() {
        if (slopeStatsAggregator.getCount() <= 1) {
            LOG.debug("No data points available for angle calculation");
            this.angle = Double.POSITIVE_INFINITY;
            return;
        }
        try {
            double sumX = slopeStatsAggregator.getSumX();
            double sumY = slopeStatsAggregator.getSumY();
            double sumXY = slopeStatsAggregator.getSumXY();
            double sumX2 = slopeStatsAggregator.getSumX2();
            long count = slopeStatsAggregator.getCount();

            // Calculate the slope (m) using OLS formula
            double numerator = (count * sumXY) - (sumX * sumY);
            double denominator = (count * sumX2) - (sumX * sumX);

            if (denominator == 0) {
                LOG.warn("Denominator for angle calculation is zero, setting angle to INFINITY");
                this.angle = Double.POSITIVE_INFINITY;
                return;
            }

            double slope = numerator / denominator;

            // Calculate the angle in degrees
            this.angle = Math.toDegrees(Math.atan(slope));
            LOG.info("Calculated angle: {}", this.angle);
        } catch (Exception e) {
            LOG.error("Error calculating angle", e);
            this.angle = Double.POSITIVE_INFINITY;
        }

    }
    
    // Accessors and utility methods
    
    @Override
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

    @Override
    public Sketch clone() {
        OLSSketch sketch = new OLSSketch(this.from, this.to, this.aggregationType);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        sketch.slopeStatsAggregator = this.slopeStatsAggregator.clone();
        return sketch;
    }

    @Override
    public boolean isEmpty() {
        return slopeStatsAggregator.getCount() == 0;
    }

    @Override
    public boolean hasInitialized() {
        return hasInitialized;
    }
    
    @Override
    public boolean matches(ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double angle = getAngle(); // Ensure angle is calculated
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return angle >= low && angle <= high;
    }

   
    public SlopeStatsAggregator getSlopeStatsAggregator() {
        return slopeStatsAggregator;
    }

    @Override
    public String toString(){
        return "OLSSketch{" +
                "from=" + getFrom() +
                ", to=" + getTo() +
                ", angle=" + angle +
                '}';
    }
}
