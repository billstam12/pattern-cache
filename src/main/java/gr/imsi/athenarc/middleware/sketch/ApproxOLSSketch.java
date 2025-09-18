package gr.imsi.athenarc.middleware.sketch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Only for non timestampe stats = agg. time series spans */
public class ApproxOLSSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(ApproxOLSSketch.class);

    private long from;
    private long to;
    private long windowId;
    
    private double angle;
    private double minAngle; // Lower bound of angle error
    private double maxAngle; // Upper bound of angle error
    private double angleErrorMargin; // The difference between max and min angles

    // used for fetching data from the db
    // There is a difference between count = 0 and no underlying data at all
    private boolean hasInitialized = false;
    
    // Track all data points for linear programming solution
    private List<ReferenceDataPoint> allDataPoints = new ArrayList<>();

    private AggregateInterval originalAggregateInterval;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param windowId The window id of this sketch, used to calculate the angle
     */
    public ApproxOLSSketch(long from, long to, long windowId) {
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
        Stats stats = dp.getStats();
        if (stats.getCount() > 0) {
            // Create and store reference data point
            double fromPositionRelativeToSketch = windowId + (dp.getFrom() - this.from) / (double) originalAggregateInterval.toDuration().toMillis();
            double toPositionRelativeToSketch = windowId + (dp.getTo() - this.from) / (double) originalAggregateInterval.toDuration().toMillis();
            ReferenceDataPoint refPoint = new ReferenceDataPoint(
                fromPositionRelativeToSketch, toPositionRelativeToSketch, dp.getStats().getMinValue(), dp.getStats().getMaxValue());
            
            // Add to all data points list for LP calculation
            allDataPoints.add(refPoint);
        }
    }
    
    /**
     * Checks if this sketch can be combined with another one.
     * The sketches must be consecutive (this.to == other.from) and have compatible aggregation types.
     * 
     * @param other The sketch to check for compatibility
     * @return true if sketches can be combined, false otherwise
     */
    @Override
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            LOG.debug("Cannot combine with null or empty sketch");
            return false;
        }
        
        if (!(other instanceof ApproxOLSSketch)) {
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
     * @throws IllegalArgumentException if the sketches are not compatible
     */
    @Override
    public Sketch combine(Sketch other) {
        // Validate input using canCombineWith
        if (!canCombineWith(other)) {
            throw new IllegalArgumentException("Cannot combine incompatible sketches");
        }
        
        ApproxOLSSketch otherSketch = (ApproxOLSSketch) other;
        
        // Update time interval and duration
        this.to = otherSketch.getTo();
        
        // Calculate angle between consecutive sketches before combining stats
        computeAngleBetweenConsecutiveSketches(this, otherSketch);
        
        // Combine all data points for LP calculations
        // The original window IDs in the reference points are preserved
        this.allDataPoints.addAll(otherSketch.allDataPoints);
    
        return this;
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



    @Override
    public Sketch clone() {
        ApproxOLSSketch sketch = new ApproxOLSSketch(this.from, this.to, this.windowId);
        sketch.hasInitialized = this.hasInitialized;
        sketch.angle = this.angle;
        sketch.minAngle = this.minAngle;
        sketch.maxAngle = this.maxAngle;
        sketch.angleErrorMargin = this.angleErrorMargin;
        sketch.allDataPoints = new ArrayList<>(this.allDataPoints);
        sketch.originalAggregateInterval = this.originalAggregateInterval;
        return sketch;
    }
    
    @Override
    public boolean isEmpty() {
        return allDataPoints.isEmpty();
    }

    @Override
    public boolean hasInitialized() {
        return hasInitialized;
    }
    
    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
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
        calculateAngleWithMinMax(combinedPoints);
        
        LOG.debug("Calculated angle between consecutive sketches {} and {} : {} (range: {} to {}), error margin: {}", 
                first.getFromDate(), second.getToDate(), this.angle, this.minAngle, this.maxAngle, this.angleErrorMargin);
    }
    
    /**
     * Calculates the slope (angle) and its error bounds using interval regression technique.
     * Also calculates standard errors of the regression coefficients in a 95% confidence interval.
     * 
     * @param dataPoints The list of reference data points to use for calculation
     */
    private void calculateAngleWithMidpoints(List<ReferenceDataPoint> dataPoints) {
        if (dataPoints.isEmpty()) {
            this.angle = Double.POSITIVE_INFINITY;
            this.minAngle = Double.POSITIVE_INFINITY;
            this.maxAngle = Double.POSITIVE_INFINITY;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;
            return;
        }
        
        // Extract x values (times), midpoints and ranges
        double[] xMidValues = new double[dataPoints.size()];
        double[] midPoints = new double[dataPoints.size()];
        
        for (int i = 0; i < dataPoints.size(); i++) {
            ReferenceDataPoint point = dataPoints.get(i);
            // This places each data point at the appropriate position based on its window
            xMidValues[i] = (point.getTo() + point.getFrom()) / 2.0; // Midpoint of the time interval
            midPoints[i] = (point.getMaxValue() + point.getMinValue()) / 2.0; // Midpoint between min and max
        }
        // Calculate regression with standard errors for midpoints
        RegressionResult midResult = calculateRegressionWithStdErrors(xMidValues, midPoints);
        double bMid = midResult.slope; // Slope for midpoints
        double midpointSlopeStdErr = midResult.slopeStdErr;
        
        //  Calculate bounds for slope
        double bLo = bMid - (midpointSlopeStdErr * 2); // 95% confidence interval lower bound
        double bHi = bMid + (midpointSlopeStdErr * 2); // 95% confidence interval upper bound

        double slopeMin = Math.min(bLo, bHi);
        double slopeMax = Math.max(bLo, bHi);

        // Now as before:
        this.minAngle = Math.toDegrees(Math.atan(slopeMin));
        this.maxAngle = Math.toDegrees(Math.atan(slopeMax));
        this.angle = (this.minAngle + this.maxAngle) / 2.0;
        this.angleErrorMargin = (this.maxAngle - this.minAngle) / 180.0;

        // Calculate angle and its bounds (in degrees)
        this.minAngle = Math.toDegrees(Math.atan(slopeMin));
        this.maxAngle = Math.toDegrees(Math.atan(slopeMax));
        this.angle = (this.minAngle + this.maxAngle) / 2.0; // Average angle
        this.angleErrorMargin = (this.maxAngle - this.minAngle) / 180.0;
    }

    private void calculateAngleWithMinMax(List<ReferenceDataPoint> dataPoints) {
         if (dataPoints.isEmpty()) {
            this.angle = Double.POSITIVE_INFINITY;
            this.minAngle = Double.POSITIVE_INFINITY;
            this.maxAngle = Double.POSITIVE_INFINITY;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;
            return;
        }
        
        // Extract x values (times), midpoints and ranges
        double[] xMidValues = new double[dataPoints.size()];

        // For each window, use min and max as possible alternative representatives
        double[] yMins = new double[dataPoints.size()];
        double[] yMaxs = new double[dataPoints.size()];

        for (int i = 0; i < dataPoints.size(); i++) {
            ReferenceDataPoint point = dataPoints.get(i);
            xMidValues[i] = (point.getTo() + point.getFrom()) / 2.0; // Midpoint of the time interval
            yMins[i] = point.getMinValue();
            yMaxs[i] = point.getMaxValue();
        }

        RegressionResult minResult = calculateRegressionWithStdErrors(xMidValues, yMins);
        RegressionResult maxResult = calculateRegressionWithStdErrors(xMidValues, yMaxs);

        // Now, your slope is somewhere in [minResult.slope, maxResult.slope]
        double slopeLower = Math.min(minResult.slope, maxResult.slope);
        double slopeUpper = Math.max(minResult.slope, maxResult.slope);

        // Use these as bounds for your angle estimate
        this.minAngle = Math.toDegrees(Math.atan(slopeLower));
        this.maxAngle = Math.toDegrees(Math.atan(slopeUpper));
        this.angle = (this.minAngle + this.maxAngle) / 2.0; // Average angle
        this.angleErrorMargin = (this.maxAngle - this.minAngle) / 180.0;
    }

    /**
     * Calculates simple linear regression coefficients and their standard errors
     * 
     * @param x Independent variable array
     * @param y Dependent variable array
     * @return RegressionResult containing slope, intercept and their standard errors
     */
    private RegressionResult calculateRegressionWithStdErrors(double[] x, double[] y) {
        int n = x.length;
        if (n <= 2) {
            // Not enough data points for meaningful standard error calculation
            return new RegressionResult(0, 0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
        }
        
        // Calculate means
        double meanX = 0.0;
        double meanY = 0.0;
        for (int i = 0; i < n; i++) {
            meanX += x[i];
            meanY += y[i];
        }
        meanX /= n;
        meanY /= n;
        
        // Calculate slope and intercept
        double sumXY = 0.0;
        double sumXX = 0.0;
        for (int i = 0; i < n; i++) {
            double xDiff = x[i] - meanX;
            sumXY += xDiff * (y[i] - meanY);
            sumXX += xDiff * xDiff;
        }
        
        double slope = sumXX != 0 ? sumXY / sumXX : 0.0;
        double intercept = meanY - slope * meanX;
        
        // Calculate residuals and residual sum of squares
        double rss = 0.0;
        for (int i = 0; i < n; i++) {
            double yPred = intercept + slope * x[i];
            double residual = y[i] - yPred;
            rss += residual * residual;
        }
        
        // Calculate standard errors
        // Degrees of freedom = n - 2 (we estimated 2 parameters: slope and intercept)
        double mse = rss / (n - 2);
        double slopeStdErr = Math.sqrt(mse / sumXX);
        double interceptStdErr = Math.sqrt(mse * (1.0/n + meanX*meanX/sumXX));
        
        return new RegressionResult(intercept, slope, interceptStdErr, slopeStdErr);
    }
    
    /**
     * Container class for regression results including standard errors
     */
    private static class RegressionResult {
        public final double intercept;
        public final double slope;
        public final double interceptStdErr;
        public final double slopeStdErr;
        
        public RegressionResult(double intercept, double slope, double interceptStdErr, double slopeStdErr) {
            this.intercept = intercept;
            this.slope = slope;
            this.interceptStdErr = interceptStdErr;
            this.slopeStdErr = slopeStdErr;
        }
    }
    
    /**
     * Gets a list of all reference data points stored in this sketch
     * 
     * @return List of all reference data points
     */
    public List<ReferenceDataPoint> getAllDataPoints() {
        return new ArrayList<>(allDataPoints);
    }

    public void addDataPoint(DataPoint dp){
        throw new UnsupportedOperationException("This sketch does not support adding individual data points directly. Use addAggregatedDataPoint instead.");
    }

    private class ReferenceDataPoint {  
        private final double from;
        private final double to;
        private final double maxValue;
        private final double minValue;

        public ReferenceDataPoint(double from, double to, double minValue, double maxValue) {
            this.from = from;
            this.to = to;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public double getFrom() {
            return from;
        }

        public double getTo(){
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
                    ", to=" + to +
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
