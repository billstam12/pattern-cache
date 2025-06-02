package gr.imsi.athenarc.middleware.cache;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PixelColumn implements Sketch {

    private static final Logger LOG = LoggerFactory.getLogger(PixelColumn.class);

    private final long from;
    private final long to;

    private final ViewPort viewPort;

    private final RangeSet<Long> fullyContainedRangeSet = TreeRangeSet.create();

    private StatsAggregator statsAggregator;
    
    private StatsAggregator fullyContainedStatsAggregator;
    
    private AggregateInterval originalAggregateInterval; 

    private AggregatedDataPoint leftPartial;
    private AggregatedDataPoint rightPartial;

    // The left and right agg data points of this pixel column. These can be either partially-contained inside this pixel column and overlap, or fully-contained.
    private List<AggregatedDataPoint> left = new ArrayList<>();
    private List<AggregatedDataPoint> right = new ArrayList<>();

    private boolean hasNoError = false;

    private boolean hasInitialized = false;

    private double angle;
     
    public void markAsNoError() {
        this.hasNoError = true;
    }

    public void markAsHasError() {
        this.hasNoError = false;
    }

    public boolean hasNoError() {
        return hasNoError;
    }

    public PixelColumn(long from, long to, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        statsAggregator = new StatsAggregator();
        fullyContainedStatsAggregator = new StatsAggregator();
        this.viewPort = viewPort;
        this.originalAggregateInterval = AggregateInterval.fromMillis(to - from);
    }

    public void addDataPoint(DataPoint dp){
        hasInitialized = true;
        statsAggregator.accept(dp);
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        hasInitialized = true;
        if (dp.getFrom() <= from) {
            left.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (dp.getTo() >= to) {
            right.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }
        Stats stats = dp.getStats();
        if (this.encloses(dp)) {
            Range<Long> guavaRange = Range.closedOpen(dp.getFrom(), dp.getTo());
            fullyContainedRangeSet.add(guavaRange);
            if (stats.getCount() > 0) {
                fullyContainedStatsAggregator.accept(stats.getMinDataPoint());
                fullyContainedStatsAggregator.accept(stats.getMaxDataPoint());
            }
        }
        if(this.overlaps(dp) && stats.getCount() > 0){
            statsAggregator.accept(stats.getMinDataPoint());
            statsAggregator.accept(stats.getMaxDataPoint());
        }
        
    }


    private void determinePartialContained() {
        Range<Long> pixelColumnTimeRange = Range.closedOpen(from, to);
        Range<Long> fullyContainedRange = fullyContainedRangeSet.span();

        ImmutableRangeSet<Long> immutableFullyContainedRangeSet = ImmutableRangeSet.copyOf(fullyContainedRangeSet);

        // Compute difference between pixel column range and fullyContainedRangeSet
        ImmutableRangeSet<Long> differenceSet = ImmutableRangeSet.of(pixelColumnTimeRange).difference(immutableFullyContainedRangeSet);

        List<Range<Long>> differenceList = differenceSet.asRanges().stream()
                .collect(Collectors.toList());

        Range<Long> leftSubRange = null;
        Range<Long> rightSubRange = null;

        if (differenceList.size() == 2) {
            leftSubRange = differenceList.get(0);
            rightSubRange = differenceList.get(1);
        } else if (differenceList.size() == 1) {
            if (differenceList.get(0).lowerEndpoint() < fullyContainedRange.lowerEndpoint()) {
                leftSubRange = differenceList.get(0);
            } else {
                rightSubRange = differenceList.get(0);
            }
        }


        if (leftSubRange != null) {
            Range<Long> finalLeftSubRange = leftSubRange;
            if(left.size() == 0) leftPartial = null;
            else {
                leftPartial = left.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getTo() >= finalLeftSubRange.upperEndpoint())
                          .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                          .orElseGet(() ->  null);                      
            }
        } else {
            leftPartial = null;
        }
        if (rightSubRange != null) {
            Range<Long> finalRightSubRange = rightSubRange;
            if(right.size() == 0) rightPartial = null;
            else
                rightPartial = right.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getFrom() <= finalRightSubRange.lowerEndpoint())
                        .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                        .orElseGet(() ->  null);
        } else {
            rightPartial = null;
        }
    }

    /**
     * Computes the maximum inner pixel range for this Pixel Column. For this we consider both the fully contained and the partially contained groups.
     * @param viewPortStats
     * @return the maximum inner column pixel range or null if there are gaps in the fully contained ranges or no fully contained ranges at all.
     */

     public Range<Integer> computeMaxInnerPixelRange(Stats viewPortStats) {
        Set<Range<Long>> fullyContainedDisjointRanges = fullyContainedRangeSet.asRanges();
        if (fullyContainedDisjointRanges.size() > 1) {
            LOG.debug("There are gaps in the fully contained ranges of this pixel column.");
            return null;
        } else if (fullyContainedDisjointRanges.size() == 0) {
            LOG.debug("There is no fully contained range in this pixel column.");
            return null;
        }
        determinePartialContained();
        if(statsAggregator.getCount() > 0) {
            int minPixelId = viewPort.getPixelId(statsAggregator.getMinValue(), viewPortStats);
            int maxPixelId = viewPort.getPixelId(statsAggregator.getMaxValue(), viewPortStats);
            if (leftPartial != null && leftPartial.getCount() > 0) {
                minPixelId = Math.min(minPixelId, viewPort.getPixelId(leftPartial.getStats().getMinValue(), viewPortStats));
                maxPixelId = Math.max(maxPixelId, viewPort.getPixelId(leftPartial.getStats().getMaxValue(), viewPortStats));
            }
            if (rightPartial != null && rightPartial.getCount() > 0 )  {
                minPixelId = Math.min(minPixelId, viewPort.getPixelId(rightPartial.getStats().getMinValue(), viewPortStats));
                maxPixelId = Math.max(maxPixelId, viewPort.getPixelId(rightPartial.getStats().getMaxValue(), viewPortStats));
            }
            return Range.closed(minPixelId, maxPixelId);
        } else { // There are no data in this pixel column
            throw new IllegalStateException("There are no data in this pixel column.");
        }
    }


    /**
     * Returns a closed range of pixel IDs that the line segment intersects within this pixel column.
     *
     * @param t1            The first timestamp of the line segment.
     * @param v1            The value at the first timestamp of the line segment.
     * @param t2            The second timestamp of the line segment.
     * @param v2            The value at the second timestamp of the line segment.
     * @param viewPortStats The stats for the entire view port.
     * @return A Range object representing the range of pixel IDs that the line segment intersects within the pixel column.
     */
    public Range<Integer> getPixelIdsForLineSegment(double t1, double v1, double t2, double v2, Stats viewPortStats) {
        // Calculate the slope of the line segment
        double slope = (v2 - v1) / (t2 - t1);

        // Calculate the y-intercept of the line segment
        double yIntercept = v1 - slope * t1;

        // Find the first and last timestamps of the line segment within the pixel column
        double tStart = Math.max(from, Math.min(t1, t2));
        double tEnd = Math.min(to, Math.max(t1, t2));

        // Calculate the values at the start and end timestamps
        double vStart = Math.max(viewPortStats.getMinValue(), slope * tStart + yIntercept);
        double vEnd = Math.min(viewPortStats.getMaxValue(), slope * tEnd + yIntercept);

        // Convert the values to pixel ids       
        int pixelIdStart = viewPort.getPixelId(vStart, viewPortStats);
        int pixelIdEnd = viewPort.getPixelId(vEnd, viewPortStats);
        // Create a range from the pixel ids and return it
        return Range.closed(Math.min(pixelIdStart, pixelIdEnd), Math.max(pixelIdStart, pixelIdEnd));
    }

    /**
     * Returns the range of inner-column pixel IDs that can be correctly determined for this pixel column for the give measure.
     * This range is determined by the min and max values over the fully contained groups in this pixel column.
     *
     * @param viewPortStats The stats for the entire view port.
     * @return A Range object representing the range of inner-column pixel IDs
     */
    public Range<Integer> getActualInnerColumnPixelRange(Stats viewPortStats) {
        if(fullyContainedStatsAggregator.getCount() <= 0) return Range.open(0, viewPort.getHeight()); // If not initialized or empty
        return Range.closed(viewPort.getPixelId(fullyContainedStatsAggregator.getMinValue(), viewPortStats),
                viewPort.getPixelId(fullyContainedStatsAggregator.getMaxValue(), viewPortStats));
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

    public Stats getStats() {
        return statsAggregator;
    }

    public AggregatedDataPoint getLeftPartial() {
        return leftPartial;
    }

    public AggregatedDataPoint getRightPartial() {
        return rightPartial;
    }

    public List<AggregatedDataPoint> getLeft() {
        return left;
    }

    public List<AggregatedDataPoint> getRight() {
        return right;
    }

    public boolean hasInitialized() {
        return hasInitialized;
    }

    @Override
    public String toString() {
        return "PixelColumn{ timeInterval: " + getIntervalString() + ", stats: " + statsAggregator + "}";
    }

    @Override
    public double getAngle() {
        return angle;
    }

    @Override
    public AggregationType getAggregationType() {
        return AggregationType.LAST_VALUE;
    }

    @Override
    public Sketch combine(Sketch other) {
        if (other instanceof PixelColumn) {
            PixelColumn otherPixelColumn = (PixelColumn) other;
            if(other.isEmpty()) {
                LOG.debug("Other PixelColumn is empty, skipping combination.");
                this.angle = Double.POSITIVE_INFINITY;
            } else {
                this.angle = (otherPixelColumn.getStats().getLastValue() - this.getStats().getLastValue()) /
                        ((otherPixelColumn.getFrom() - this.getFrom()) / (this.originalAggregateInterval.toDuration().toMillis()));
                LOG.debug("Calculated angle for PixelColumn from {} to {}: {}", this.from, this.to, this.angle);
            }
            this.statsAggregator.combine(otherPixelColumn.statsAggregator);
            this.fullyContainedStatsAggregator.combine(otherPixelColumn.fullyContainedStatsAggregator);
            this.left.addAll(otherPixelColumn.left.stream().map(ImmutableAggregatedDataPoint::fromAggregatedDataPoint).collect(Collectors.toList()));
            this.right.addAll(otherPixelColumn.right.stream().map(ImmutableAggregatedDataPoint::fromAggregatedDataPoint).collect(Collectors.toList()));
            this.hasNoError = this.hasNoError && otherPixelColumn.hasNoError;
            this.hasInitialized = this.hasInitialized || otherPixelColumn.hasInitialized;
            return this; 
        } else {
            LOG.warn("Cannot combine PixelColumn with non-PixelColumn sketch: {}", other.getClass().getSimpleName());
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return this.statsAggregator.getCount() == 0;
    }

    @Override
    public boolean matches(ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double angle = getAngle();
        double low = filter.getMinDegree();
        double high = filter.getMaxDegree();
        return angle >= low && angle <= high;
    }


    public PixelColumn clone() {
        PixelColumn clone = new PixelColumn(this.from, this.to, this.viewPort);
        clone.statsAggregator = this.statsAggregator.clone();
        clone.fullyContainedStatsAggregator = this.fullyContainedStatsAggregator.clone();
        clone.leftPartial = leftPartial == null ? null : ImmutableAggregatedDataPoint.fromAggregatedDataPoint(this.leftPartial);
        clone.rightPartial = rightPartial == null ? null : ImmutableAggregatedDataPoint.fromAggregatedDataPoint(this.rightPartial);
        clone.left.addAll(this.left.stream().map(ImmutableAggregatedDataPoint::fromAggregatedDataPoint).collect(Collectors.toList()));
        clone.right.addAll(this.right.stream().map(ImmutableAggregatedDataPoint::fromAggregatedDataPoint).collect(Collectors.toList()));
        clone.hasNoError = this.hasNoError;
        clone.hasInitialized = this.hasInitialized;
        return clone;
    }
}