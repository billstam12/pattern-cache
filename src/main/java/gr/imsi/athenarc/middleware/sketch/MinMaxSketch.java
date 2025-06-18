package gr.imsi.athenarc.middleware.sketch;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/** Only for non timestampe stats = agg. time series spans */
public class MinMaxSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(M4Sketch.class);

    private long from;
    private long to;

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
        
        if (!(other instanceof MinMaxSketch)) {
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
        
        MinMaxSketch otherSketch = (MinMaxSketch) other;

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
            if (this.lastDataPoint == null || otherSketch.lastDataPoint.getFrom() > this.lastDataPoint.getFrom()) {
                this.lastDataPoint = otherSketch.lastDataPoint;
            }
        }
                
        return this;
    }

    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
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


    @Override
    public boolean isEmpty() {
        return firstDataPoint == null;
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
    private void computeAngleBetweenConsecutiveSketches(MinMaxSketch first, MinMaxSketch second) {
        ReferenceDataPoint firstPoint = first.getReferencePointFromSketch();
        ReferenceDataPoint secondPoint = second.getReferencePointFromSketch();
        LOG.debug("Calculating angle between sketches from {} to {}", firstPoint, secondPoint);
        if (firstPoint == null || secondPoint == null) {
            LOG.debug("Insufficient data points to calculate angle between sketches");
            this.angle = Double.POSITIVE_INFINITY;;
            this.minAngle = Double.POSITIVE_INFINITY;;
            this.maxAngle = Double.POSITIVE_INFINITY;;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;;
            return;
        }
        // Calculate time change
        long timeChange = (secondPoint.getFrom() - firstPoint.getFrom()) / originalAggregateInterval.toDuration().toMillis(); // Convert to milliseconds

        if (timeChange == 0) {
            LOG.warn("Zero time difference between reference points, setting angle to 0");
            this.angle = Double.POSITIVE_INFINITY;;
            this.minAngle = Double.POSITIVE_INFINITY;;
            this.maxAngle = Double.POSITIVE_INFINITY;;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;;
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
        
        LOG.info("First point min-max values: {}-{} real value: {}, Second point min-max values: {}-{}real value: {}",
            firstPoint.getMinValue(), firstPoint.getMaxValue(), firstPoint.getValue(),
            secondPoint.getMinValue(), secondPoint.getMaxValue(), secondPoint.getValue());
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
