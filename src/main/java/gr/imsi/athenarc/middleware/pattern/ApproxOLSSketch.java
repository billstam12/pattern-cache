package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.Sketch;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.distribution.TDistribution;

/** Only for non timestampe stats = agg. time series spans */
public class ApproxOLSSketch implements Sketch {

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
    
    // Track all data points for linear programming solution
    private List<ReferenceDataPoint> allDataPoints = new ArrayList<>();

    // The interval this sketch represents. If it is combined it is the interval of its sub-sketches that created it.
    // Used in angle calculation.
    private AggregateInterval originalAggregateInterval; 

    // Confidence level for the angle error calculation (95%)
    private static final double CONFIDENCE_LEVEL = 0.5;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public ApproxOLSSketch(long from, long to, AggregationType aggregationType) {
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
            // Create and store reference data point
            ReferenceDataPoint refPoint = new ReferenceDataPoint(
                dp.getFrom(), dp.getTo(), dp.getStats().getMinValue(), dp.getStats().getMaxValue());
            
            // Add to all data points list for LP calculation
            allDataPoints.add(refPoint);
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

        if (!(other instanceof ApproxOLSSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        ApproxOLSSketch otherSketch = (ApproxOLSSketch) other;
        validateConsecutiveSketch(otherSketch);
        validateCompatibleAggregationTypes(otherSketch);
        
        // Update time interval and duration
        this.to = otherSketch.getTo();
        
        // Calculate angle between consecutive sketches before combining stats
        computeAngleBetweenConsecutiveSketches(this, otherSketch);
    
        
        // Combine all data points for LP calculations
        this.allDataPoints.addAll(otherSketch.allDataPoints);
    
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
        ApproxOLSSketch sketch = new ApproxOLSSketch(this.from, this.to, this.aggregationType);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.minAngle = this.minAngle;
        sketch.maxAngle = this.maxAngle;
        sketch.angleErrorMargin = this.angleErrorMargin;
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        sketch.allDataPoints = new ArrayList<>(this.allDataPoints);
        return sketch;
    }

    public boolean isEmpty() {
        return allDataPoints.size() == 0;
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

    /**
     * Calculates the angle between two consecutive sketches based on their first or last data points,
     * depending on aggregation type.
     * 
     * @param first The first sketch in sequence
     * @param second The second sketch in sequence (consecutive to first)
     */
    private void computeAngleBetweenConsecutiveSketches(ApproxOLSSketch first, ApproxOLSSketch second) {
        // Combine data points from both sketches for LP calculation
        List<ReferenceDataPoint> combinedPoints = new ArrayList<>(first.allDataPoints);
        combinedPoints.addAll(second.allDataPoints);
        
        if (combinedPoints.isEmpty()) {
            LOG.debug("No data points available to calculate angle between sketches");
            this.angle = Double.POSITIVE_INFINITY;
            this.minAngle = Double.POSITIVE_INFINITY;
            this.maxAngle = Double.POSITIVE_INFINITY;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;
            return;
        }
        
        // Calculate the slope and its confidence bounds using interval regression
        calculateAngleWithErrorBounds(combinedPoints);
        
        LOG.debug("Calculated angle between consecutive sketches {} and {} : {} (range: {} to {}), error margin: {}", 
                first, second, this.angle, this.minAngle, this.maxAngle, this.angleErrorMargin);
    }
    
    /**
     * Calculates the slope (angle) and its error bounds using interval regression technique.
     * This implements the NM (new method) interval regression from the Python reference code.
     * 
     * @param dataPoints The list of reference data points to use for calculation
     */
    private void calculateAngleWithErrorBounds(List<ReferenceDataPoint> dataPoints) {
        if (dataPoints.isEmpty()) {
            this.angle = Double.POSITIVE_INFINITY;
            this.minAngle = Double.POSITIVE_INFINITY;
            this.maxAngle = Double.POSITIVE_INFINITY;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;
            return;
        }
        
        // Extract x values (times), midpoints and ranges
        double[] xValues = new double[dataPoints.size()];
        double[] midPoints = new double[dataPoints.size()];
        double[] ranges = new double[dataPoints.size()];
        
        for (int i = 0; i < dataPoints.size(); i++) {
            ReferenceDataPoint point = dataPoints.get(i);
            xValues[i] = (point.getTo() - this.getFrom()) / (double)(this.getTo() - this.getFrom()); // Use middle of time interval as x-coordinate
            midPoints[i] = (point.getMaxValue() + point.getMinValue()) / 2.0; // Midpoint between min and max
            ranges[i] = (point.getMaxValue() - point.getMinValue()) / 2.0; // Half - Range (max - min) / 2
        }
        
        // Perform midpoint regression (y_mid = a_mid + b_mid * x)
        OLSMultipleLinearRegression midRegression = new OLSMultipleLinearRegression();
        double[][] xDesign = createDesignMatrix(xValues);
        midRegression.newSampleData(midPoints, xDesign);
        double[] midParams = midRegression.estimateRegressionParameters();
        double[] midStdErrors = midRegression.estimateRegressionParametersStandardErrors();
        double aMid = midParams[0]; // Intercept
        double bMid = midParams[1]; // Slope
        double saMid = midStdErrors[0]; // Standard error of intercept
        double sbMid = midStdErrors[1]; // Standard error of slope
        
        // Perform range regression (y_range = a_range + b_range * x)
        OLSMultipleLinearRegression rangeRegression = new OLSMultipleLinearRegression();
        rangeRegression.newSampleData(ranges, xDesign);
        double[] rangeParams = rangeRegression.estimateRegressionParameters();
        double[] rangeStdErrors = rangeRegression.estimateRegressionParametersStandardErrors();
        double aRange = rangeParams[0]; // Intercept
        double bRange = rangeParams[1]; // Slope
        double saRange = rangeStdErrors[0]; // Standard error of intercept
        double sbRange = rangeStdErrors[1]; // Standard error of slope
        
        // Calculate bounds for slope
        double bLo = bMid - bRange; // based on the paper
        double bHi = bMid + bRange; // based on the paper

        // double sCommon = Math.sqrt(sbMid * sbMid + 0.25 * sbRange * sbRange);
        // Critical value from t-distribution
        // int degreesOfFreedom = dataPoints.size() - 2;
        // TDistribution tDist = new TDistribution(degreesOfFreedom);
        // double tCrit = tDist.inverseCumulativeProbability(1 - (1 - CONFIDENCE_LEVEL) / 2);
        
        // // Calculate confidence intervals for lower and upper bounds of slope
        // double[] ciLo = new double[] {bLo - tCrit * sCommon, bLo + tCrit * sCommon};
        // double[] ciHi = new double[] {bHi - tCrit * sCommon, bHi + tCrit * sCommon};
        
        // Calculate final slope bounds as the min/max of the confidence intervals
        // double slopeMin = Math.min(ciLo[0], ciHi[0]);
        // double slopeMax = Math.max(ciLo[1], ciHi[1]);

        double slopeMin = Math.min(bLo, bHi);
        double slopeMax = Math.max(bLo, bHi);

        // Calculate angle and its bounds (in degrees)
        this.minAngle = Math.toDegrees(Math.atan(slopeMin));
        this.maxAngle = Math.toDegrees(Math.atan(slopeMax));
        this.angle = (this.minAngle + this.maxAngle) / 2.0; // Average angle
        this.angleErrorMargin = (this.maxAngle - this.minAngle) / 180.0;
        
        LOG.info("Calculated angle: {} degrees (min: {} degrees, max: {} degrees, error margin: {}, from {} data points)", 
                this.angle, this.minAngle, this.maxAngle, this.angleErrorMargin, dataPoints.size());
    }
    
    /**
     * Creates a design matrix for linear regression with a constant term (intercept)
     * 
     * @param x The array of x values
     * @return A 2D array with the design matrix
     */
    private double[][] createDesignMatrix(double[] x) {
        double[][] design = new double[x.length][1];
        for (int i = 0; i < x.length; i++) {
            design[i][0] = x[i]; // x value
        }
        return design;
    }
    
    /**
     * Gets a list of all reference data points stored in this sketch
     * 
     * @return List of all reference data points
     */
    public List<ReferenceDataPoint> getAllDataPoints() {
        return new ArrayList<>(allDataPoints);
    }

    private class ReferenceDataPoint {  
        private final long from;
        private final long to;
        private final double maxValue;
        private final double minValue;

        public ReferenceDataPoint(long from, long to, double minValue, double maxValue) {
            this.from = from;
            this.to = to;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public long getFrom() {
            return from;
        }

        public long getTo(){
            return to;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public double getMinValue() {
            return minValue;
        }

        @Override
        public String toString() {
            return "ReferenceDataPoint{" +
                    "from=" + from +
                    "to=" + to +
                    ", minValue=" + minValue +
                    ", maxValue=" + maxValue +
                    '}';
        }
    }

    @Override
    public String toString(){
        return "NonTimestampedSketch{" +
                "from=" + getFromDate() +
                ", to=" + getToDate() +
                '}';
    }
}
