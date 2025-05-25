package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;

/** Only for non timestampe stats = agg. time series spans */
public class NonTimestampedSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(NonTimestampedSketch.class);

    private long from;
    private long to;

    // Store the stats from each interval
    private StatsAggregator statsAggregator = new StatsAggregator();
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;
    
    private double slope;
    private double minSlope; // Lower bound of slope error
    private double maxSlope; // Upper bound of slope error
    private double slopeErrorMargin; // The difference between max and min slopes

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // Track first and last data points for slope calculation
    private ReferenceDataPoint firstDataPoint;
    private ReferenceDataPoint lastDataPoint;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public NonTimestampedSketch(long from, long to, AggregationType aggregationType) {
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
            if (firstDataPoint == null || dp.getTimestamp() < firstDataPoint.getFrom()) {
                firstDataPoint = new ReferenceDataPoint(
                    dp.getFrom(), dp.getTo(), dp.getStats().getFirstValue());
            }
            if (lastDataPoint == null || dp.getTimestamp() > lastDataPoint.getFrom()) {
                lastDataPoint = new ReferenceDataPoint(
                    dp.getFrom(), dp.getTo(), dp.getStats().getLastValue());
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

        if (!(other instanceof NonTimestampedSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        NonTimestampedSketch otherSketch = (NonTimestampedSketch) other;
        validateConsecutiveSketch(otherSketch);
        validateCompatibleAggregationTypes(otherSketch);

        // Calculate slope between consecutive sketches before combining stats
        computeSlopeBetweenConsecutiveSketches(this, otherSketch);
        // Update time interval and duration
        this.to = otherSketch.getTo();
        
        // Update first/last data points
        if (otherSketch.firstDataPoint != null) {
            if (this.firstDataPoint == null || otherSketch.firstDataPoint.getFrom() < this.firstDataPoint.getFrom()) {
                this.firstDataPoint = otherSketch.firstDataPoint;
            }
        }
        
        if (otherSketch.lastDataPoint != null) {
            if (this.lastDataPoint == null || otherSketch.lastDataPoint.getFrom()  > this.lastDataPoint.getFrom() ) {
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
    
    /**
     * Gets the minimum possible slope based on error bounds calculation.
     *
     * @return The minimum possible slope value
     */
    public double getMinSlope() {
        return minSlope;
    }
    
    /**
     * Gets the maximum possible slope based on error bounds calculation.
     *
     * @return The maximum possible slope value
     */
    public double getMaxSlope() {
        return maxSlope;
    }
    
    /**
     * Gets the error margin in the slope calculation.
     *
     * @return The error margin (difference between max and min slopes)
     */
    public double getSlopeErrorMargin() {
        return slopeErrorMargin;
    }

    public Sketch clone() {
        NonTimestampedSketch sketch = new NonTimestampedSketch(this.from, this.to, this.aggregationType);
        sketch.statsAggregator = this.statsAggregator.clone();
        sketch.hasInitialized = this.hasInitialized;
        sketch.slope = this.slope;
        sketch.minSlope = this.minSlope;
        sketch.maxSlope = this.maxSlope;
        sketch.slopeErrorMargin = this.slopeErrorMargin;
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
    public ReferenceDataPoint getFirstAddedDataPoint() {
        return firstDataPoint;
    }
    
    /**
     * Gets the last data point added to this sketch.
     * 
     * @return The last aggregated data point
     */
    public ReferenceDataPoint getLastAddedDataPoint() {
        return lastDataPoint;
    }
    
    /**
     * Calculates the slope between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeSlopeBetweenConsecutiveSketches(NonTimestampedSketch first, NonTimestampedSketch second) {
        ReferenceDataPoint firstPoint = first.getReferencePointFromSketch();
        ReferenceDataPoint secondPoint = second.getReferencePointFromSketch();
        LOG.debug("Calculating slope between sketches from {} to {}", firstPoint, secondPoint);
        if (firstPoint == null || secondPoint == null) {
            LOG.debug("Insufficient data points to calculate slope between sketches");
            this.slope = 0.0;
            this.minSlope = 0.0;
            this.maxSlope = 0.0;
            this.slopeErrorMargin = 0.0;
            return;
        }
        // Calculate value change
        double valueChange = secondPoint.getValue() - firstPoint.getValue();
        // Calculate time change
        long timeChange = secondPoint.getFrom()  - firstPoint.getFrom();

        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting slope to 0");
            this.slope = 0.0;
            this.minSlope = 0.0;
            this.maxSlope = 0.0;
            this.slopeErrorMargin = 0.0;
            return;
        }
        
        // Calculate error bounds based on possible timestamp ranges
        calculateSlopeErrorBounds(first, second, firstPoint, secondPoint, valueChange);
        
        LOG.debug("Calculated slope between consecutive sketches {} and {} : {} (range: {} to {}), error margin: {}", 
                first.getFromDate(), second.getFromDate(), this.slope, this.minSlope, this.maxSlope, this.slopeErrorMargin);
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
    private ReferenceDataPoint getReferencePointFromSketch() {
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
    
    /**
     * Calculates error bounds for slope based on possible timestamp ranges of the data points
     * within each sketch.
     *
     * @param first First sketch in the sequence
     * @param second Second sketch in the sequence
     * @param firstPoint Reference point from first sketch
     * @param secondPoint Reference point from second sketch
     * @param valueChange The value change between reference points
     */
    private void calculateSlopeErrorBounds(Sketch first, Sketch second, 
                                           ReferenceDataPoint firstPoint, 
                                           ReferenceDataPoint secondPoint,
                                           double valueChange) {
        // For each reference point, determine its potential time range within its sketch
        long firstPointEarliestPossibleTime, firstPointLatestPossibleTime;
        long secondPointEarliestPossibleTime, secondPointLatestPossibleTime;
        
        // Calculate time range for the first point based on aggregation type
        if (first.getAggregationType() == AggregationType.FIRST_VALUE) {
            // For first value, it could be anywhere from the start of the sketch to its recorded timestamp
            firstPointEarliestPossibleTime = first.getFrom();
            firstPointLatestPossibleTime = firstPoint.getTo();
        } else if (first.getAggregationType() == AggregationType.LAST_VALUE) {
            // For last value, it could be anywhere from its recorded timestamp to the end of the sketch
            firstPointEarliestPossibleTime = firstPoint.getFrom();
            firstPointLatestPossibleTime = first.getTo();
        } else {
            // For min/max values, it could be anywhere within the sketch
            firstPointEarliestPossibleTime = first.getFrom();
            firstPointLatestPossibleTime = first.getTo();
        }
        
        // Calculate time range for the second point based on aggregation type
        if (second.getAggregationType() == AggregationType.FIRST_VALUE) {
            // For first value, it could be anywhere from the start of the sketch to its recorded timestamp
            secondPointEarliestPossibleTime = second.getFrom();
            secondPointLatestPossibleTime = secondPoint.getTo();
        } else if (second.getAggregationType() == AggregationType.LAST_VALUE) {
            // For last value, it could be anywhere from its recorded timestamp to the end of the sketch
            secondPointEarliestPossibleTime = secondPoint.getFrom();
            secondPointLatestPossibleTime = second.getTo();
        } else {
            // For min/max values, it could be anywhere within the sketch
            secondPointEarliestPossibleTime = second.getFrom();
            secondPointLatestPossibleTime = second.getTo();
        }

        // Calculate the maximum possible time change (for min slope)
        // This is when the first point is at its earliest and the second point is at its latest
        long maxTimeChange = secondPointLatestPossibleTime - firstPointEarliestPossibleTime;
        
        // Calculate the minimum possible time change (for max slope)
        // This is when the first point is at its latest and the second point is at its earliest
        long minTimeChange = secondPointEarliestPossibleTime - firstPointLatestPossibleTime;
        
        // Ensure min time change is at least 1 to avoid division by zero
        minTimeChange = Math.max(1, minTimeChange);
        
        // Calculate the normalized time changes for error bounds
        double minNormalizedTimeChange = minTimeChange / (double)(second.getTo() - first.getFrom());
        double maxNormalizedTimeChange = maxTimeChange / (double)(second.getTo() - first.getFrom());

        // Calculate raw slopes for the bounds
        double rawMinSlope = valueChange / maxNormalizedTimeChange;
        double rawMaxSlope = valueChange / minNormalizedTimeChange;
        
        // Ensure proper ordering if value change is negative
        if (valueChange < 0) {
            double temp = rawMinSlope;
            rawMinSlope = rawMaxSlope;
            rawMaxSlope = temp;
        }
        
        // Normalize to range [-0.5, 0.5]
        this.minSlope = Math.atan(rawMinSlope) / Math.PI;
        this.maxSlope = Math.atan(rawMaxSlope) / Math.PI;
        this.slope = (this.minSlope + this.maxSlope) / 2.0;
        this.slopeErrorMargin = Math.abs(this.maxSlope - this.minSlope);
    }

    private class ReferenceDataPoint {  
        private final long from;
        private final long to;
        private final double value;

        public ReferenceDataPoint(long from, long to, double value) {
            this.from = from;
            this.to = to;
            this.value = value;
        }

        public long getFrom() {
            return from;
        }

        public long getTo(){
            return to;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ReferenceDataPoint{" +
                    "from=" + from +
                    "to=" + to +
                    ", value=" + value +
                    '}';
        }
    }
}
