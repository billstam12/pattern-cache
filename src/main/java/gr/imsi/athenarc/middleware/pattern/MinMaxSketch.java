package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.Sketch;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** Only for non timestampe stats = agg. time series spans */
public class MinMaxSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(M4Sketch.class);

    private long from;
    private long to;

    // Store the stats from each interval
    private StatsAggregator statsAggregator = new StatsAggregator();
    
    // The aggregation type to use when adding data points
    private AggregationType aggregationType;
    
    private double angle;
    private double minAngle; // Lower bound of angle error
    private double maxAngle; // Upper bound of angle error
    private double angleErrorMargin; // The difference between max and min angles

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;

    // Track first and last data points for angle calculation
    private ReferenceDataPoint firstDataPoint;
    private ReferenceDataPoint lastDataPoint;

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
    public MinMaxSketch(long from, long to, AggregationType aggregationType) {
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
            // Track first and last data points for angle calculation
            if (firstDataPoint == null || dp.getFrom() < firstDataPoint.getFrom()) { 
                firstDataPoint = new ReferenceDataPoint(
                    dp.getFrom(), dp.getTo(), dp.getStats().getFirstValue(), dp.getStats().getMinValue(), dp.getStats().getMaxValue());  
            }
            if (lastDataPoint == null || dp.getFrom() > lastDataPoint.getFrom()) {
                lastDataPoint = new ReferenceDataPoint(
                    dp.getFrom(), dp.getTo(), dp.getStats().getLastValue(), dp.getStats().getMinValue(), dp.getStats().getMaxValue());  
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

        if (!(other instanceof MinMaxSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        MinMaxSketch otherSketch = (MinMaxSketch) other;
        validateConsecutiveSketch(otherSketch);
        validateCompatibleAggregationTypes(otherSketch);

        // Calculate angle between consecutive sketches before combining stats
        computeAngleBetweenConsecutiveSketches(this, otherSketch);
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
    public double getAngle() {
        return angle;
    }
    
    /**
     * Gets the minimum possible angle based on error bounds calculation.
     *
     * @return The minimum possible angle value
     */
    public double getMinAngle() {
        return minAngle;
    }
    
    /**
     * Gets the maximum possible angle based on error bounds calculation.
     *
     * @return The maximum possible angle value
     */
    public double getMaxAngle() {
        return maxAngle;
    }
    
    /**
     * Gets the error margin in the angle calculation.
     *
     * @return The error margin (difference between max and min angles)
     */
    public double getAngleErrorMargin() {
        return angleErrorMargin;
    }

    public Sketch clone() {
        MinMaxSketch sketch = new MinMaxSketch(this.from, this.to, this.aggregationType);
        sketch.statsAggregator = this.statsAggregator.clone();
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.minAngle = this.minAngle;
        sketch.maxAngle = this.maxAngle;
        sketch.angleErrorMargin = this.angleErrorMargin;
        sketch.firstDataPoint = this.firstDataPoint;
        sketch.lastDataPoint = this.lastDataPoint;
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
     * Computes the slope of a composite sketch against the ValueFilter of a segment.
     * Returns true if the slope is within the filter's range.
     */
    public boolean matches(ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        boolean match = minAngle >= low && maxAngle <= high;
        return match;
    }

    /**
     * Calculates the angle between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeAngleBetweenConsecutiveSketches(MinMaxSketch first, MinMaxSketch second) {
        ReferenceDataPoint firstPoint = first.getReferencePointFromSketch();
        ReferenceDataPoint secondPoint = second.getReferencePointFromSketch();
        LOG.debug("Calculating angle between sketches from {} to {}", firstPoint, secondPoint);
        if (firstPoint == null || secondPoint == null) {
            LOG.debug("Insufficient data points to calculate angle between sketches");
            this.angle = 0.0;
            this.minAngle = 0.0;
            this.maxAngle = 0.0;
            this.angleErrorMargin = 0.0;
            return;
        }
        // Calculate time change
        long timeChange = (secondPoint.getFrom() - firstPoint.getFrom()) / originalAggregateInterval.toDuration().toMillis(); // Convert to milliseconds

        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting angle to 0");
            this.angle = 0.0;
            this.minAngle = 0.0;
            this.maxAngle = 0.0;
            this.angleErrorMargin = 0.0;
            return;
        }
        
        // Calculate error bounds based on possible timestamp ranges
        calculateAngleErrorBounds(first, second, firstPoint, secondPoint, timeChange);
        
        LOG.debug("Calculated angle between consecutive sketches {} and {} : {} (range: {} to {}), error margin: {}", 
                first, second, this.angle, this.minAngle, this.maxAngle, this.angleErrorMargin);
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
     * Calculates error bounds for angle based on possible timestamp ranges of the data points
     * within each sketch.
     *
     * @param first First sketch in the sequence
     * @param second Second sketch in the sequence
     * @param firstPoint Reference point from first sketch
     * @param secondPoint Reference point from second sketch
     * @param timeChange The time change between reference points
     */
    private void calculateAngleErrorBounds(Sketch first, Sketch second, 
                                           ReferenceDataPoint firstPoint, 
                                           ReferenceDataPoint secondPoint,
                                           double timeChange) {
                
                                        
        double valueChange = secondPoint.getValue() - firstPoint.getValue();                                    
        // Calculate the maximum possible value change (for max angle)
        // This is when the first point value is at its minimum and the second point is at its maximum
        double maxValueChange = secondPoint.getMaxValue() - firstPoint.getMinValue();
        
        // Calculate the minimum possible value change (for min angle)
        // This is when the first point is at its maximum and the second point is at its minimum
        double minValueChange = secondPoint.getMinValue() - firstPoint.getMaxValue();
        

        // Calculate slopes for error bounds
        double normalizedTimeChange = timeChange;
        double minSlope = minValueChange / normalizedTimeChange;
        double maxSlope = maxValueChange / normalizedTimeChange;
        
        // Calculate degree angles
        double rawAngle = Math.atan(valueChange / normalizedTimeChange) * (180.0 / Math.PI);
        this.minAngle = Math.atan(minSlope) * (180.0 / Math.PI);
        this.maxAngle = Math.atan(maxSlope) * (180.0 / Math.PI);
        this.angle = rawAngle;
        
        // Calculate error margin as a percentage of the angle
        this.angleErrorMargin = this.angle != 0 ? 
            (Math.abs(this.maxAngle - this.minAngle) / Math.abs(this.angle)) : 0.0;
        
        LOG.debug("Minimum angle: {}, Maximum angle: {}, Angle: {}", 
            this.minAngle, this.maxAngle, this.angle);
    }

    private class ReferenceDataPoint {  
        private final long from;
        private final long to;
        private final double minValue;
        private final double maxValue;

        private final double value;

        public ReferenceDataPoint(long from, long to, double value, double minValue, double maxValue) {
            this.from = from;
            this.to = to;
            this.value = value; //for test
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public long getFrom() {
            return from;
        }

        public double getValue() {
            return value;
        }

        public long getTo(){
            return to;
        }

        public double getMinValue() {
            return minValue;
        }

        public double getMaxValue() {
            return maxValue;
        }

        @Override
        public String toString() {
            return "ReferenceDataPoint{" +
                    ", from= " + from +
                    ", to= " + to +
                    ", minValue= " + minValue +
                    ", maxValue= " + maxValue +
                    ", value=" + value +
                    '}';
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
