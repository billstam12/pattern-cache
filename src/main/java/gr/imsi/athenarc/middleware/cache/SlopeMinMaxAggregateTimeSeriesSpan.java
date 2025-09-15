package gr.imsi.athenarc.middleware.cache;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.OLSSlopeMinMaxStats;
import gr.imsi.athenarc.middleware.domain.OLSSlopeStats;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * A TimeSeriesSpan that, in addition to slope stats, also stores min/max aggregates per interval.
 */
public class SlopeMinMaxAggregateTimeSeriesSpan implements TimeSeriesSpan {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpan.class);
    private int measure;
    private int count;
    private long[] aggregates;
    private long from;
    private long to;
    private int size;
    private AggregateInterval aggregateInterval;
    private boolean isInit = false;
    private static int AGG_SIZE = 7; // sum_x, sum_y, sum_xy, sum_x2, count, min, max

    private void initialize(long from, long to, AggregateInterval aggregateInterval, int measure) {
        this.size = DateTimeUtil.numberOfIntervals(from, to, aggregateInterval);
        this.from = from;
        this.to = to;
        this.aggregateInterval = aggregateInterval;
        this.measure = measure;
        this.aggregates = new long[size * AGG_SIZE];
        LOG.debug("Initializing min/max time series span ({},{}) measure = {} with size {}, aggregate interval {}", getFromDate(), getToDate(), measure, size, aggregateInterval);
    }

    protected SlopeMinMaxAggregateTimeSeriesSpan(long from, long to, int measure, AggregateInterval aggregateInterval) {
        initialize(from, to, aggregateInterval, measure);
    }

    public void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint) {
        int i = DateTimeUtil.indexInInterval(getFrom(), getTo(), aggregateInterval, aggregatedDataPoint.getTimestamp());
        Stats stats = aggregatedDataPoint.getStats();
        if (!(stats instanceof OLSSlopeMinMaxStats)) {
            LOG.error("Expected OLSSlopeMinMaxStats but got {}", stats.getClass().getSimpleName());
            return;
        }
        OLSSlopeMinMaxStats slopeStats = (OLSSlopeMinMaxStats) stats;
        if (stats.getCount() == 0) {
            return;
        }
        count += stats.getCount();
        int aggregateCount = stats.getCount();
        LOG.debug("Adding min/max data point at index {} with count {}, timestamp: {}", i, aggregateCount, aggregatedDataPoint.getTimestamp());
        if (i < 0 || i >= size) {
            LOG.error("Invalid index {} for aggregates array of size {}", i, size);
            return;
        }
        aggregates[i * AGG_SIZE] = Double.doubleToRawLongBits(slopeStats.getSumX());
        aggregates[i * AGG_SIZE + 1] = Double.doubleToRawLongBits(slopeStats.getSumY());
        aggregates[i * AGG_SIZE + 2] = Double.doubleToRawLongBits(slopeStats.getSumXY());
        aggregates[i * AGG_SIZE + 3] = Double.doubleToRawLongBits(slopeStats.getSumX2());
        aggregates[i * AGG_SIZE + 4] = aggregateCount;
        aggregates[i * AGG_SIZE + 5] = Double.doubleToRawLongBits(slopeStats.getMinValue());
        aggregates[i * AGG_SIZE + 6] = Double.doubleToRawLongBits(slopeStats.getMaxValue());
    }

    private int getIndex(final long timestamp) {
        int index = DateTimeUtil.indexInInterval(getFrom(), getTo(), aggregateInterval, timestamp);
        if (index >= size) {
            return size - 1;
        } else if (index < 0) {
            return 0;
        }
        return index;
    }

    public int getSize() { return size; }
    public AggregateInterval getAggregateInterval() { return aggregateInterval; }
    public TimeInterval getResidual() { return new TimeRange(from + (aggregateInterval.toDuration().toMillis()) * (size - 1), to); }
    public Iterator<AggregatedDataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) { return new TimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp); }
    public TimeRange getTimeRange() { return new TimeRange(getFrom(), getTo()); }
    
    @Override
    public Iterator<DataPoint> iterator() {
        final Iterator<AggregatedDataPoint> aggIt = iterator(from, -1);
        return new Iterator<DataPoint>() {
            @Override
            public boolean hasNext() { return aggIt.hasNext(); }
            @Override
            public DataPoint next() { return aggIt.next(); }
        };
    }

     /**
     * Rolls up this span to a coarser granularity by aggregating data points, including min/max.
     * This method creates a new span with a larger aggregation interval.
     *
     * @param targetInterval The target aggregation interval to roll up to
     * @return A new span with rolled-up data, or null if the operation isn't possible
     */
    public SlopeMinMaxAggregateTimeSeriesSpan rollUp(AggregateInterval targetInterval) {
        if (targetInterval.toDuration().toMillis() <= aggregateInterval.toDuration().toMillis()) {
            LOG.warn("Cannot roll up to a finer or equal granularity interval");
            return null;
        }
        long currentMs = aggregateInterval.toDuration().toMillis();
        long targetMs = targetInterval.toDuration().toMillis();
        if (targetMs % currentMs != 0) {
            LOG.warn("Target interval must be an even multiple of the current interval");
            return null;
        }
        int factor = (int) (targetMs / currentMs);
        SlopeMinMaxAggregateTimeSeriesSpan rolledUpSpan = new SlopeMinMaxAggregateTimeSeriesSpan(from, to, measure, targetInterval);
        int newSize = (size + factor - 1) / factor;
        LOG.debug("Rolling up min/max span from interval {} to {} (ratio 1:{}) - old size: {}, new size: {}", aggregateInterval, targetInterval, factor, size, newSize);
        for (int newIdx = 0; newIdx < newSize; newIdx++) {
            final int startIdx = newIdx * factor;
            final int endIdx = Math.min((newIdx + 1) * factor, size);
            if (startIdx >= size) {
                break;
            }
            double total_sum_x = 0.0;
            double total_sum_y = 0.0;
            double total_sum_xy = 0.0;
            double total_sum_x2 = 0.0;
            int totalCount = 0;
            double minValue = Double.POSITIVE_INFINITY;
            double maxValue = Double.NEGATIVE_INFINITY;
            boolean hasValue = false;
            for (int i = startIdx; i < endIdx; i++) {
                int count = (int) aggregates[i * AGG_SIZE + 4];
                if (count > 0) {
                    total_sum_x += Double.longBitsToDouble(aggregates[i * AGG_SIZE]);
                    total_sum_y += Double.longBitsToDouble(aggregates[i * AGG_SIZE + 1]);
                    total_sum_xy += Double.longBitsToDouble(aggregates[i * AGG_SIZE + 2]);
                    total_sum_x2 += Double.longBitsToDouble(aggregates[i * AGG_SIZE + 3]);
                    totalCount += count;
                    double min = Double.longBitsToDouble(aggregates[i * AGG_SIZE + 5]);
                    double max = Double.longBitsToDouble(aggregates[i * AGG_SIZE + 6]);
                    if (min < minValue) minValue = min;
                    if (max > maxValue) maxValue = max;
                    hasValue = true;
                }
            }
            if (!hasValue || totalCount == 0) {
                continue;
            }
            final long timestamp = from + (newIdx * targetInterval.toDuration().toMillis());
            final double finalSumX = total_sum_x;
            final double finalSumY = total_sum_y;
            final double finalSumXY = total_sum_xy;
            final double finalSumX2 = total_sum_x2;
            final int finalCount = totalCount;
            final double finalMin = minValue;
            final double finalMax = maxValue;
            AggregatedDataPoint aggPoint = new AggregatedDataPoint() {
                @Override public int getMeasure() { return measure; }
                @Override public long getTimestamp() { return timestamp; }
                @Override public Stats getStats() {
                    return new OLSSlopeMinMaxStats(finalSumX, finalSumY, finalSumXY, finalSumX2, finalCount, finalMin, finalMax);
                }
                @Override public long getFrom() { return timestamp; }
                @Override public long getTo() { return timestamp + targetInterval.toDuration().toMillis(); }
                @Override public double getValue() { throw new UnsupportedOperationException(); }
                @Override public int getCount() { return finalCount; }
            };
            rolledUpSpan.addAggregatedDataPoint(aggPoint);
        }
        return rolledUpSpan;
    }

    @Override
    public long calculateDeepMemorySize() {
        // Memory overhead for an object in a 64-bit JVM
        final int OBJECT_OVERHEAD = 16;
        final int ARRAY_OVERHEAD = 24;
        final int INT_SIZE = 4;
        final int LONG_SIZE = 8;
        final int REF_SIZE = 8;
        long baseSize = OBJECT_OVERHEAD;
        baseSize += INT_SIZE;  // measure
        baseSize += INT_SIZE;  // count
        baseSize += REF_SIZE;  // aggregates reference
        baseSize += LONG_SIZE; // from
        baseSize += LONG_SIZE; // to
        baseSize += INT_SIZE;  // size
        baseSize += REF_SIZE;  // aggregateInterval reference
        long aggregatesSize = ARRAY_OVERHEAD + (aggregates.length * LONG_SIZE);
        long aggregateIntervalSize = OBJECT_OVERHEAD + LONG_SIZE + REF_SIZE;
        return baseSize + aggregatesSize + aggregateIntervalSize;
    }
    @Override public long getFrom() { return from; }
    @Override public long getTo() { return to; }
    @Override public String toString() { return "{[" + getFromDate() + "(" + getFrom() + ")" + ", " + getToDate() + "(" + getTo() + ")" + "), size=" + size + ", measures =" + measure + "aggregateInterval=" + aggregateInterval + "}"; }
    public static int getAggSize() { return AGG_SIZE; }
    @Override public int getCount() { return count; }
    @Override public int getMeasure() { return measure; }
    public boolean isInit() { return isInit; }
    public void setInit(boolean init) { isInit = init; }

    private class TimeSeriesSpanIterator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {
        private Iterator<Integer> internalIt;
        private long timestamp;
        private int currentIndex = -1;
        public TimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp - 1) + 1 : size).iterator();
        }
        @Override public boolean hasNext() { return internalIt.hasNext(); }
        @Override public AggregatedDataPoint next() {
            currentIndex = internalIt.next();
            timestamp = from + currentIndex * aggregateInterval.toDuration().toMillis();
            return this;
        }
        @Override public int getCount() { return (int) aggregates[currentIndex * AGG_SIZE + 4]; }
        @Override public Stats getStats() {
            return new OLSSlopeStats() {

                private int index = currentIndex;

                @Override
                public int getCount() {
                    return (int) aggregates[index * AGG_SIZE + 4];
                }

                @Override
                public double getSumX() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE]);
                }

                @Override
                public double getSumY() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 1]);
                }

                @Override
                public double getSumXY() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 2]);
                }

                @Override
                public double getSumX2() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 3]);
                }

                @Override
                public double getMinValue() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 5]);
                }   

                @Override
                public double getMaxValue() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 6]);
                }

                 @Override
                public long getMinTimestamp() {
                    return (getFrom() + getTo()) / 2;
                }

                @Override
                public long getMaxTimestamp() {
                    return (getFrom() + getTo()) / 2;
                }

                @Override
                public double getFirstValue() {
                    return (getMinValue() + getMaxValue()) / 2;
                }

                @Override
                public long getFirstTimestamp() {
                    return getFrom() + 1;
                }

                @Override
                public double getLastValue() {
                    return (getMinValue() + getMaxValue()) / 2;
                }

                @Override
                public long getLastTimestamp() {
                    return getTo() - 1;
                }
            };
        }
        @Override public int getMeasure() { return measure; }
        @Override public long getTimestamp() { return timestamp; }
        @Override public long getFrom() { return timestamp; }
        @Override public long getTo() { if (currentIndex == size - 1) { return to; } else { return from + (currentIndex + 1) * aggregateInterval.toDuration().toMillis(); } }
        @Override public double getValue() { throw new UnsupportedOperationException(); }
    }
}
