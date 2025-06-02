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
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

/** Only for non timestampe stats = agg. time series spans */
public class MidrangeSketch implements Sketch {

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
    private static final double CONFIDENCE_LEVEL = 0.95;

    /**
     * Creates a new sketch with the specified aggregation type.
     *
     * @param from The start timestamp of this sketch
     * @param to The end timestamp of this sketch
     * @param aggregationType The function that gets the representative data point
     */
    public MidrangeSketch(long from, long to, AggregationType aggregationType) {
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

        if (!(other instanceof MidrangeSketch)) {
            throw new IllegalArgumentException("Cannot combine sketches of different types: " + other.getClass());
        }
        
        MidrangeSketch otherSketch = (MidrangeSketch) other;
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
        MidrangeSketch sketch = new MidrangeSketch(this.from, this.to, this.aggregationType);
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
    private void computeAngleBetweenConsecutiveSketches(MidrangeSketch first, MidrangeSketch second) {
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
     * Solves the envelope slopes linear programming problem.
     * This is a direct port of the Python envelope_slopes function.
     * 
     * @param x The x coordinates (time points)
     * @param low The lower bounds of the y values
     * @param high The upper bounds of the y values
     * @return Array containing [maxSlope, maxIntercept, minSlope, minIntercept]
     */
    private double[] solveEnvelopeSlopes(double[] x, double[] low, double[] high) {
        int n = x.length;
        
        // Direct implementation of the Python code without normalization
        // This matches exactly: envelope_slopes(x, y_lo, y_hi)
        List<LinearConstraint> constraints = new ArrayList<>();
        
        // Add constraints for each data point
        for (int i = 0; i < n; i++) {
            // Upper bound: m*x + c <= high[i]
            // This is the first half of: np.vstack([np.column_stack([x, np.ones(n)]), ...])
            double[] upperRow = new double[] { x[i], 1.0 };
            constraints.add(new LinearConstraint(upperRow, Relationship.LEQ, high[i]));
            
            // Lower bound: m*x + c >= low[i] => -m*x - c <= -low[i]
            // This is the second half of: np.vstack([..., np.column_stack([-x, -np.ones(n)])])
            double[] lowerRow = new double[] { -x[i], -1.0 };
            constraints.add(new LinearConstraint(lowerRow, Relationship.LEQ, -low[i]));
        }
        
        // Create linear constraint set
        LinearConstraintSet constraintSet = new LinearConstraintSet(constraints);
        
        try {
            // Find maximum slope line (res_max = linprog(c=[-1, 0], A_ub=A, b_ub=b))
            LinearObjectiveFunction maxObj = new LinearObjectiveFunction(new double[] { -1.0, 0.0 }, 0);
            PointValuePair maxSolution = new SimplexSolver().optimize(
                new MaxIter(1000),
                maxObj, 
                constraintSet, 
                GoalType.MINIMIZE
            );
            
            // Find minimum slope line (linprog(c=[1, 0], A_ub=A, b_ub=b))
            LinearObjectiveFunction minObj = new LinearObjectiveFunction(new double[] { 1.0, 0.0 }, 0);
            PointValuePair minSolution = new SimplexSolver().optimize(
                new MaxIter(1000),
                minObj, 
                constraintSet, 
                GoalType.MINIMIZE
            );
            
            // Extract solutions 
            double mMax = maxSolution.getPoint()[0]; 
            double cMax = maxSolution.getPoint()[1];
            double mMin = minSolution.getPoint()[0];
            double cMin = minSolution.getPoint()[1];
                        
            return new double[] { mMax, cMax, mMin, cMin };
            
        } catch (Exception e) {
            LOG.error("Linear programming failed: {}. Falling back to heuristic estimation.", e.getMessage());
            
            // Fallback: Use a heuristic approach if LP fails
            // Simple approach: Find slopes between min-min points and max-max points
            double mMin = Double.POSITIVE_INFINITY;
            double mMax = Double.NEGATIVE_INFINITY;
            
            // Calculate all possible pairwise slopes
            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    if (Math.abs(x[j] - x[i]) > 1e-6) { // Avoid division by near-zero
                        // Slopes between all combinations of min and max points
                        double[] slopes = new double[4];
                        slopes[0] = (low[j] - low[i]) / (x[j] - x[i]); // min-min
                        slopes[1] = (high[j] - high[i]) / (x[j] - x[i]); // max-max
                        slopes[2] = (low[j] - high[i]) / (x[j] - x[i]); // min-max
                        slopes[3] = (high[j] - low[i]) / (x[j] - x[i]); // max-min
                        
                        for (double slope : slopes) {
                            mMin = Math.min(mMin, slope);
                            mMax = Math.max(mMax, slope);
                        }
                    }
                }
            }
            
            // If still no solution, use a default
            if (mMin == Double.POSITIVE_INFINITY || mMax == Double.NEGATIVE_INFINITY) {
                mMin = -1.0; 
                mMax = 1.0;
                LOG.warn("Fallback failed. Using default slopes: min={}, max={}", mMin, mMax);
            }
            
            // Calculate intercepts using midpoints
            double avgX = 0, avgLow = 0, avgHigh = 0;
            for (int i = 0; i < n; i++) {
                avgX += x[i];
                avgLow += low[i];
                avgHigh += high[i];
            }
            avgX /= n;
            avgLow /= n;
            avgHigh /= n;
            
            double avgY = (avgLow + avgHigh) / 2;
            double cMin = avgY - mMin * avgX;
            double cMax = avgY - mMax * avgX;
            
            LOG.debug("Using fallback heuristic: Max slope = {}, Min slope = {}", mMax, mMin);
            return new double[] { mMax, cMax, mMin, cMin };
        }
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
        
        try {
            // Extract x (time) and y (min/max) values from dataPoints
            int n = dataPoints.size();
            double[] x = new double[n];
            double[] yLow = new double[n];
            double[] yHigh = new double[n];
            
            // Find first timestamp to use as base (0)
            long baseTimestamp = dataPoints.get(0).getFrom();
            
            for (int i = 0; i < n; i++) {
                ReferenceDataPoint dp = dataPoints.get(i);
                // Convert timestamps to a reasonable unit (like the Python example)
                // Use a simple transformation: (timestamp - first_timestamp) / scale
                // where scale normalizes to a reasonable range like 0-60
                double midpoint = (dp.getFrom() + dp.getTo()) / 2.0;
                double scale = 60000.0; // This puts timestamps in minute range
                x[i] = (midpoint - baseTimestamp) / scale;
                yLow[i] = dp.getMinValue();
                yHigh[i] = dp.getMaxValue();
            }
            
            // Log the values to help with debugging
            if (LOG.isDebugEnabled()) {
                LOG.debug("X values (normalized time): {}", java.util.Arrays.toString(x));
                LOG.debug("Y low values: {}", java.util.Arrays.toString(yLow));
                LOG.debug("Y high values: {}", java.util.Arrays.toString(yHigh));
            }
            
            // Solve linear programming problem for min and max slopes
            double[] result = solveEnvelopeSlopes(x, yLow, yHigh);
            double mMax = result[0];
            double cMax = result[1];
            double mMin = result[2];
            double cMin = result[3];
            
            // Calculate best slope and uncertainty (as in Python code)
            double mBest = (mMax + mMin) / 2.0;
            double uncertainty = (mMax - mMin) / 2.0;
            
            // Convert slope to angle (in degrees)
            this.angle = Math.toDegrees(Math.atan(mBest));
            this.minAngle = Math.toDegrees(Math.atan(mMin));
            this.maxAngle = Math.toDegrees(Math.atan(mMax));
            this.angleErrorMargin = Math.abs(this.maxAngle - this.minAngle);
            
            LOG.debug("Linear programming solution - Max slope: {}, Min slope: {}, Best: {}, Uncertainty: {}", 
                    mMax, mMin, mBest, uncertainty);
        } catch (Exception e) {
            LOG.error("Error calculating angle using linear programming: {}", e.getMessage());
            this.angle = Double.POSITIVE_INFINITY;
            this.minAngle = Double.POSITIVE_INFINITY;
            this.maxAngle = Double.POSITIVE_INFINITY;
            this.angleErrorMargin = Double.POSITIVE_INFINITY;
        }
        
        LOG.debug("Calculated angle: {} degrees (min: {} degrees, max: {} degrees, error margin: {})", 
                this.angle, this.minAngle, this.maxAngle, this.angleErrorMargin);
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
