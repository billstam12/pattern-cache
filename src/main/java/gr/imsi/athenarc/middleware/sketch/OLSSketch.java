package gr.imsi.athenarc.middleware.sketch;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.OLSSlopeStats;
import gr.imsi.athenarc.middleware.domain.OLSSlopeStatsAggregator;

/** 
 * A sketch that uses Ordinary Least Squares regression to compute the slope (angle) 
 * of a time series segment.
 */
public class OLSSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(OLSSketch.class);

    private long from;
    private long to;
    
    private double angle;
    private boolean angleComputed = false;

    // used for fetching data from the db
    private boolean hasInitialized = false;
    
    // The interval this sketch represents
    private AggregateInterval originalAggregateInterval;
    
    private OLSSlopeStatsAggregator slopeStatsAggregator = new OLSSlopeStatsAggregator();

    private long windowId; // Identifier for the window this sketch belongs to

    /**
     * Creates a new OLS sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public OLSSketch(long from, long to, long windowId) {
        this.from = from;
        this.to = to;
        this.windowId = windowId;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
    }

    /**
     * Adds an aggregated data point to this sketch, using the configured aggregation type.
     *
     * @param dp The aggregated data point to add
     */
    @Override
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true; // Mark as having underlying data
        OLSSlopeStats dpStats = (OLSSlopeStats) dp.getStats();

        // The dp's stats now arrive in the dataset-anchored coordinate system: x is the
        // absolute "bucket_index_from_alignedStart + fractional_offset" emitted by the
        // slope-aggregate SQL (SqlLikeSlopeAggregatedDatapoints). No additive shift by
        // windowId is needed any more — the SQL already includes it.
        //
        // The only transform left is a multiplicative rescaling when dp's bucket width
        // (`dpInterval`) is finer than this sketch's parent interval (`sketchInterval`)
        // — e.g. a cached span hands back sub-bucket aggregates. In that case the SQL's
        // x is in *dp-interval* units, so we multiply by dpScale = dpInterval/sketchInterval
        // to land in sketch-interval units. For a full-bucket dp (dpInterval ==
        // sketchInterval) dpScale = 1 and the sums pass through unchanged.
        double sketchInterval = (double) originalAggregateInterval.toDuration().toMillis();
        double dpInterval = (double) (dp.getTo() - dp.getFrom());
        double dpScale = dpInterval / sketchInterval;

        int count = dpStats.getCount();
        double sumX = dpStats.getSumX();
        double sumY = dpStats.getSumY();
        double sumXY = dpStats.getSumXY();
        double sumX2 = dpStats.getSumX2();

        double sumX_prime  = dpScale * sumX;
        double sumX2_prime = dpScale * dpScale * sumX2;
        double sumXY_prime = dpScale * sumXY;
        double sumY_prime  = sumY;

        OLSSlopeStats transformedStats = new OLSSlopeStats(
                sumX_prime, sumY_prime, sumXY_prime, sumX2_prime, count);
        slopeStatsAggregator.accept(transformedStats);
    }
    
    /**
     * Checks if this sketch can be combined with another sketch.
     * 
     * @param other The sketch to check compatibility with
     * @return true if sketches can be combined, false otherwise
     */
    @Override
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            LOG.debug("Cannot combine with null or empty sketch");
            return false;
        }
        
        if (!(other instanceof OLSSketch)) {
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

        OLSSketch otherSketch = (OLSSketch) other;

        this.to = otherSketch.getTo();
        slopeStatsAggregator.combine(otherSketch.getSlopeStatsAggregator());
        this.angleComputed = false;

        return this;
    }
    
    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
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
            LOG.debug("Calculated angle for sketches: {} to {} - {}",this.getFromDate(), this.getToDate(),  this.angle);
        } catch (Exception e) {
            LOG.error("Error calculating angle", e);
            this.angle = Double.POSITIVE_INFINITY;
        }

    }
    

    public void addDataPoint(DataPoint dp){
        throw new UnsupportedOperationException("This sketch does not support adding individual data points directly. Use addAggregatedDataPoint instead.");
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
        if (!angleComputed) {
            calculateAngle();
            angleComputed = true;
        }
        return angle;
    }

    @Override
    public Sketch clone() {
        OLSSketch sketch = new OLSSketch(this.from, this.to, this.windowId);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.angleComputed = this.angleComputed;
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
    
    public OLSSlopeStatsAggregator getSlopeStatsAggregator() {
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
