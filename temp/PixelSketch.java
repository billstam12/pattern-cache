package gr.imsi.athenarc.middleware.sketch;

import com.google.common.collect.Range;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PixelSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(PixelSketch.class);

    private long from;
    private long to;
    
    private AggregateInterval originalAggregateInterval; 

    private double angle;

    private List<Range<Integer>> pixelColumnRange = new ArrayList<>();

    private ViewPort viewPort;

    private Stats viewPortStats;

    private PixelSketch(long from, long to) {
        this.from = from;
        this.to = to;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
    }

    public PixelSketch(PixelColumn pixelColumn, ViewPort viewPort, Stats stats) {
        this(pixelColumn.getFrom(), pixelColumn.getTo(), pixelColumn.getPixelColumnRange(), viewPort, stats);
    }

    private PixelSketch(long from, long to, Range<Integer> pixelColumnRange, ViewPort viewPort, Stats stats) {
        this.from = from;
        this.to = to;
        this.pixelColumnRange.add(pixelColumnRange);
        this.viewPort = viewPort;
        this.viewPortStats = stats;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    public TimeInterval getRange(){
        return new TimeRange(from, to);
    }


    @Override
    public double getAngle() {
        return angle;
    }

    /**
     * Checks if this sketch can be combined with another sketch.
     * 
     * @param other The sketch to check
     * @return true if sketches can be combined, false otherwise
     */
    @Override
    public boolean canCombineWith(Sketch other) {
        if (other == null || other.isEmpty()) {
            LOG.debug("Cannot combine with null or empty sketch");
            return false;
        }

        if (!(other instanceof PixelSketch)) {
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
     * Combines this pixel sketch with another sketch.
     */
    @Override
    public Sketch combine(Sketch other) {
        if (!canCombineWith(other)) {
            LOG.warn("Cannot combine incompatible sketches");
            return this;
        }

        if (other instanceof PixelSketch) {
            PixelSketch otherPixelSketch = (PixelSketch) other;
            
            if (other.isEmpty()) {
                LOG.debug("Other PixelSketch is empty, skipping combination.");
                this.angle = Double.POSITIVE_INFINITY;
            } else {
                // Merge all pixel ranges from both sketches - keep all sub-visualizations
                List<Range<Integer>> combinedPixelRanges = new ArrayList<>();
                combinedPixelRanges.addAll(this.pixelColumnRange);
                combinedPixelRanges.addAll(otherPixelSketch.pixelColumnRange);
                
                // Compute POLYLINE GRADIENT based on all pixel ranges
                if (!combinedPixelRanges.isEmpty() && this.viewPort != null && this.viewPortStats != null) {
                    
                    // Calculate the gradient of the polyline formed by connecting all pixel range centers
                    this.angle = computePolylineGradient(combinedPixelRanges, this.viewPort, this.viewPortStats);
                    
                    LOG.debug("Calculated POLYLINE GRADIENT for combined PixelSketch: {} (based on {} pixel ranges)", 
                             this.angle, combinedPixelRanges.size());
                } else {
                    // Fallback if no pixel ranges available
                    this.angle = 0.0;
                    LOG.debug("No pixel ranges available, setting angle to 0.0");
                }
                
                // Update the combined pixel ranges
                this.pixelColumnRange = combinedPixelRanges;
                
                // Preserve visualization context from the other sketch if this one doesn't have it
                if (this.viewPort == null && otherPixelSketch.viewPort != null) {
                    this.viewPort = otherPixelSketch.viewPort;
                    this.viewPortStats = otherPixelSketch.viewPortStats;
                }
            }
            
            // Update the end timestamp to span the combined range
            this.to = otherPixelSketch.getTo();
            
            return this; 
        } else {
            LOG.warn("Cannot combine PixelSketch with non-PixelSketch sketch: {}", other.getClass().getSimpleName());
        }
        return this;
    }
    
    /**
     * Computes the gradient of the polylines using pixel-based technique from notebook analysis.
     * This implements the same logic that showed good results in the Python notebook.
     */
    private double computePolylineGradient(List<Range<Integer>> pixelRanges, ViewPort viewPort, Stats viewPortStats) {
        if (pixelRanges == null || pixelRanges.isEmpty()) {
            return 0.0;
        }
        
        if (pixelRanges.size() == 1) {
            return 0.0;
        }
        
        // Step 1: Extract pixel centers and convert to values, times
        List<Double> pixelCenters = new ArrayList<>();
        List<Double> scaledValues = new ArrayList<>();
        List<Double> timeCoordinates = new ArrayList<>();

        LOG.debug("ViewPort height: {}, Stats range: {} to {}", 
                 viewPort.getHeight(), viewPortStats.getMinValue(), viewPortStats.getMaxValue());
        
        for (int i = 0; i < pixelRanges.size(); i++) {
            // Calculate pixel center (average of row indices where pixels are)
            Range<Integer> range = pixelRanges.get(i);
            double pixelCenter = (range.lowerEndpoint() + range.upperEndpoint()) / 2.0;
            pixelCenters.add(pixelCenter);
            
            LOG.debug("Original pixel range: {} to {}, center: {}", 
                     range.lowerEndpoint(), range.upperEndpoint(), pixelCenter);
            
            // Convert to actual data values using proper scaling
            double actualValue = getValueFromPixelId(viewPort, (int) pixelCenter, viewPortStats);
            scaledValues.add(actualValue);
            timeCoordinates.add((double) i);

            LOG.debug("Pixel center: {} -> Actual value: {}", pixelCenter, actualValue);
        }
        
        LOG.debug("Time coordinates: {}", timeCoordinates);
        LOG.debug("Scaled values: {}", scaledValues);
        
        // Step 2: Perform linear regression to compute slope
        double n = scaledValues.size();
        double sumX = timeCoordinates.stream().mapToDouble(Double::doubleValue).sum();
        double sumY = scaledValues.stream().mapToDouble(Double::doubleValue).sum();
        double sumXY = 0.0;
        double sumXX = 0.0;
        
        for (int i = 0; i < scaledValues.size(); i++) {
            double x = timeCoordinates.get(i);
            double y = scaledValues.get(i);
            sumXY += x * y;
            sumXX += x * x;
        }
        
        // Calculate slope using least squares: slope = (n*sumXY - sumX*sumY) / (n*sumXX - sumX*sumX)
        double slope;
        double denominator = n * sumXX - sumX * sumX;
        if (Math.abs(denominator) < 1e-10) {
            slope = 0.0;
        } else {
            slope = (n * sumXY - sumX * sumY) / denominator;
        }
        
        double averageGradient = slope;
        
        // Step 4: Convert to angle using atan (for pattern matching compatibility)
        double angle = Math.toDegrees(Math.atan(averageGradient));

        LOG.debug("Linear regression slope computed: {} degrees (slope: {})",
                 angle, averageGradient);
        LOG.debug("Pixel centers: {}", pixelCenters);
        LOG.debug("Scaled values: {}", scaledValues);
        
        return angle;
    }

    /**
     * Convert a pixel ID back to its corresponding value.
     * This is the inverse of ViewPort.getPixelId().
     */
    private static double getValueFromPixelId(ViewPort viewPort, int pixelId, Stats viewPortStats) {
        double valueRange = viewPortStats.getMaxValue() - viewPortStats.getMinValue();
        double normalizedPixel = (double) pixelId / (viewPort.getHeight() - 1);
        return viewPortStats.getMinValue() + normalizedPixel * valueRange;
    }

    @Override
    public boolean isEmpty() {
        return pixelColumnRange.size() == 0;
    }

    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
    }

    public PixelSketch clone() {
        PixelSketch clone = new PixelSketch(from, to);
        clone.originalAggregateInterval = this.originalAggregateInterval;
        clone.angle = this.angle;
        clone.pixelColumnRange = new ArrayList<>(this.pixelColumnRange);
        return clone;
    }

    public List<Range<Integer>> getPixelColumnRange() {
        return pixelColumnRange;
    }

    public void setPixelColumnRange(List<Range<Integer>> pixelColumnRange) {
        this.pixelColumnRange = pixelColumnRange;
    }

    @Override
    public void addDataPoint(DataPoint dp) {
        throw new UnsupportedOperationException("No need to add data points to PixelSketch");
    }

    @Override
    public void addAggregatedDataPoint(AggregatedDataPoint dataPoint) {
        throw new UnsupportedOperationException("No need to add aggregated data points to PixelSketch");
    }

    @Override
    public boolean hasInitialized() {
        return true;
    }
    
}