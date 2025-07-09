package gr.imsi.athenarc.middleware.sketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;

public class MinMaxSketch implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxSketch.class);

    private long from;
    private long to;

    private double angle;
    private double minAngle;
    private double maxAngle;
    private double angleErrorMargin;

    private boolean hasInitialized = false;

    private List<ReferenceDataPoint> dataPoints = new ArrayList<>();

    private AggregateInterval originalAggregateInterval;

    public MinMaxSketch(long from, long to) {
        this.from = from;
        this.to = to;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true;
        Stats stats = dp.getStats();
        if (stats.getCount() > 0) {
            dataPoints.add(new ReferenceDataPoint(
                dp.getFrom(), dp.getTo(), stats.getMinValue(), stats.getMaxValue()
            ));
        }
    }

    @Override
    public Sketch combine(Sketch other) {
        if (!canCombineWith(other)) return this;

        MinMaxSketch otherSketch = (MinMaxSketch) other;
        this.dataPoints.addAll(otherSketch.dataPoints);
        this.to = otherSketch.getTo();

        computeApproximateAngle();

        return this;
    }

    private void computeApproximateAngle() {
        if (dataPoints.size() < 2) {
            LOG.warn("Insufficient data points for angle calculation.");
            setAnglesToInfinity();
            return;
        }

        ReferenceDataPoint firstPoint = dataPoints.get(0);
        ReferenceDataPoint lastPoint = dataPoints.get(dataPoints.size() - 1);

        long timeChange = (lastPoint.getFrom() - firstPoint.getFrom()) / originalAggregateInterval.toDuration().toMillis();

        if (timeChange == 0) {
            LOG.warn("Zero time difference for angle calculation.");
            setAnglesToInfinity();
            return;
        }

        double normalizedTimeChange = (double) timeChange;

        // Compute overall trend using min/max envelope approach
        // Find the overall min and max bounds across the entire time series
        double globalMin = Double.POSITIVE_INFINITY;
        double globalMax = Double.NEGATIVE_INFINITY;
        
        for (ReferenceDataPoint point : dataPoints) {
            globalMin = Math.min(globalMin, point.getMinValue());
            globalMax = Math.max(globalMax, point.getMaxValue());
        }
        
        // Compute trend lines for min and max envelopes
        double minEnvelopeStart = firstPoint.getMinValue();
        double minEnvelopeEnd = lastPoint.getMinValue();
        double maxEnvelopeStart = firstPoint.getMaxValue();
        double maxEnvelopeEnd = lastPoint.getMaxValue();
        
        // Calculate value changes based on envelope trends
        double minValueChange = minEnvelopeEnd - minEnvelopeStart;
        double maxValueChange = maxEnvelopeEnd - maxEnvelopeStart;
        
        // Compute approximate trend considering both envelope bounds
        // Weight the central tendency based on the envelope spread
        double envelopeSpread = globalMax - globalMin;
        double approximateValueChange;
        
        if (envelopeSpread > 0) {
            // Use weighted average of min and max trends, with more weight on the envelope with larger absolute change
            double minWeight = Math.abs(minValueChange) / (Math.abs(minValueChange) + Math.abs(maxValueChange) + 1e-10);
            double maxWeight = Math.abs(maxValueChange) / (Math.abs(minValueChange) + Math.abs(maxValueChange) + 1e-10);
            approximateValueChange = minWeight * minValueChange + maxWeight * maxValueChange;
        } else {
            // Fallback to simple average if no spread
            approximateValueChange = (minValueChange + maxValueChange) / 2;
        }

        double minSlope = minValueChange / normalizedTimeChange;
        double maxSlope = maxValueChange / normalizedTimeChange;

          // Calculate midpoints of first and last data points
        double firstPointMidpoint = (firstPoint.getMinValue() + firstPoint.getMaxValue()) / 2;
        double lastPointMidpoint = (lastPoint.getMinValue() + lastPoint.getMaxValue()) / 2;
        
        // Calculate slope using the two midpoints
        double midpointSlope = (lastPointMidpoint - firstPointMidpoint) / normalizedTimeChange;
        
        this.angle = Math.atan(approximateValueChange / normalizedTimeChange) * (180.0 / Math.PI);
        this.minAngle = Math.atan(minSlope) * (180.0 / Math.PI);
        this.maxAngle = Math.atan(maxSlope) * (180.0 / Math.PI);

        this.angleErrorMargin = this.angle != 0 ? 
            (Math.abs(this.maxAngle - this.minAngle) / Math.abs(this.angle)) : 0.0;

        LOG.debug("Envelope-based angle calculated: {}, Min angle: {}, Max angle: {}, Error margin: {}, " +
                 "Min envelope change: {}, Max envelope change: {}, Approximate change: {}",
            angle, minAngle, maxAngle, angleErrorMargin, minValueChange, maxValueChange, approximateValueChange);
    }

    private void setAnglesToInfinity() {
        this.angle = Double.POSITIVE_INFINITY;
        this.minAngle = Double.POSITIVE_INFINITY;
        this.maxAngle = Double.POSITIVE_INFINITY;
        this.angleErrorMargin = Double.POSITIVE_INFINITY;
    }

    @Override
    public boolean canCombineWith(Sketch other) {
        return other instanceof MinMaxSketch && this.getTo() == other.getFrom();
    }

    @Override
    public boolean isEmpty() {
        return dataPoints.isEmpty();
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

    public double getMinAngle() {
        return minAngle;
    }

    public double getMaxAngle() {
        return maxAngle;
    }

    public double getAngleErrorMargin() {
        return angleErrorMargin;
    }

    @Override
    public Sketch clone() {
        MinMaxSketch clone = new MinMaxSketch(this.from, this.to);
        clone.dataPoints = new ArrayList<>(this.dataPoints);
        clone.angle = this.angle;
        clone.minAngle = this.minAngle;
        clone.maxAngle = this.maxAngle;
        clone.angleErrorMargin = this.angleErrorMargin;
        clone.hasInitialized = this.hasInitialized;
        clone.originalAggregateInterval = this.originalAggregateInterval;
        return clone;
    }

    private class ReferenceDataPoint {  
        long from, to;
        double minValue, maxValue;

        ReferenceDataPoint(long from, long to, double minValue, double maxValue) {
            this.from = from;
            this.to = to;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        long getFrom() { return from; }
        double getMinValue() { return minValue; }
        double getMaxValue() { return maxValue; }
    }

    @Override
    public void addDataPoint(DataPoint dp) {
        throw new UnsupportedOperationException("Unimplemented method 'addDataPoint'");
    }

    @Override
    public boolean hasInitialized() {
        return hasInitialized;
    }

    @Override
    public Optional<AggregateInterval> getOriginalAggregateInterval() {
        return Optional.ofNullable(originalAggregateInterval);
    }
}
