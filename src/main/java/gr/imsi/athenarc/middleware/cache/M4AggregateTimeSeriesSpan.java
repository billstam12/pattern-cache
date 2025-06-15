package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * A {@link DataPoints} implementation that aggregates a series of consecutive
 * raw data points based on the specified aggregation interval.
 * For each aggregation interval included we store 9 doubles,
 * i.e. the first, last, min and max aggregate values, 4 longs corresponding to their timestamp
 *  as well as the corresponding
 * non-missing value counts.
 */
public class M4AggregateTimeSeriesSpan implements TimeSeriesSpan {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpan.class);

    private int measure;

    private int count;

    /**
     * The aggregate values for every window interval 
     */
    private long[] aggregates;

    // The start time value of the span
    private long from;

    // The end time value of the span
    // Keep in mind that the end time is not included in the span,
    // and also that the end time is not necessarily aligned with the aggregation interval
    private long to;

    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;

    /**
     * The fixed window that raw data points are grouped by in this span.
     * Note that due to rounding, the last group of the span may cover a larger time interval
     */
    private AggregateInterval aggregateInterval;

    private static int AGG_SIZE = 9;

    private void initialize(long from, long to, AggregateInterval aggregateInterval, int measure) {
        this.size = DateTimeUtil.numberOfIntervals(from, to, aggregateInterval);
        this.from = from;
        this.to = to;
        this.aggregateInterval = aggregateInterval;
        this.measure = measure;
        this.aggregates = new long[size * AGG_SIZE];
        LOG.debug("Initializing time series span ({},{}) measure = {} with size {}, aggregate interval {}", getFromDate(), getToDate(), measure, size, aggregateInterval);
    }

    protected M4AggregateTimeSeriesSpan(long from, long to, int measure, AggregateInterval aggregateInterval) {
        initialize(from, to, aggregateInterval, measure);
    }

    protected void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint) {
        int i = DateTimeUtil.indexInInterval(getFrom(), getTo(), aggregateInterval, aggregatedDataPoint.getTimestamp());
        Stats stats = aggregatedDataPoint.getStats();
        if(!(stats instanceof StatsAggregator)){
            throw new IllegalArgumentException("Stats must be an instance of StatsAggregator");
        }

        if (stats.getCount() == 0) {
            return;
        }
    
        count += stats.getCount();
        int aggregateCount = stats.getCount();
        
        // Add detailed logging
        LOG.debug("Adding data point at index {} with count {}, timestamp: {}", 
                 i, aggregateCount, aggregatedDataPoint.getTimestamp());
        
        // Check if index is valid
        if (i < 0 || i >= size) {
            LOG.error("Invalid index {} for aggregates array of size {}", i, size);
            return;
        }
        
        long minTimestamp = stats.getMinTimestamp();
        long maxTimestamp = stats.getMaxTimestamp();
        long firstTimestamp = stats.getFirstTimestamp();
        long lastTimestamp = stats.getLastTimestamp();

        double minValue = stats.getMinValue();
        double maxValue = stats.getMaxValue();
        double firstValue = stats.getFirstValue();
        double lastValue = stats.getLastValue();
        
        aggregates[i * AGG_SIZE] = Double.doubleToRawLongBits(minValue);
        aggregates[i * AGG_SIZE + 1] = minTimestamp;
        aggregates[i * AGG_SIZE + 2] = Double.doubleToRawLongBits(maxValue);
        aggregates[i * AGG_SIZE + 3] = maxTimestamp;
        // first and last values for the interval
        aggregates[i * AGG_SIZE + 4] = Double.doubleToRawLongBits(firstValue);
        aggregates[i * AGG_SIZE + 5] = Double.doubleToRawLongBits(lastValue);
        aggregates[i * AGG_SIZE + 6] = firstTimestamp;
        aggregates[i * AGG_SIZE + 7] = lastTimestamp;

        aggregates[i * AGG_SIZE + 8] = aggregateCount;
    }

    /**
     * Finds the index in the span in which the given timestamp should be.
     * If the timestamp is before the start of the span, the first index is returned.
     * If the timestamp is after the end of the span, the last index is returned.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = DateTimeUtil.indexInInterval(getFrom(), getTo(), aggregateInterval, timestamp);
        if (index >= size) {
            return size - 1;
        } else if (index < 0) {
            return 0;
        }
        return index;
    }

    public int getSize() {
        return size;
    }

    public AggregateInterval getAggregateInterval() {
        return aggregateInterval;
    }

    public TimeInterval getResidual(){
        return new TimeRange(from + (aggregateInterval.toDuration().toMillis()) * (size - 1), to);
    }
    /**
     * Returns an iterator over the aggregated data points in this span that fall within the given time range.
     *
     * @param queryStartTimestamp The start timestamp of the query.
     * @param queryEndTimestamp   The end timestamp of the query (not included).
     * @return The iterator.
     */
    public Iterator<AggregatedDataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new TimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }


    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }

    @Override
    public Iterator iterator() {
        return iterator(from, -1);
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
    public String toString() {
        return "{[" + getFromDate() + "(" + getFrom() + ")" +
                ", " + getToDate() + "(" + getTo() + ")" +
                "), size=" + size + ", measures =" + measure + "aggregateInterval=" + aggregateInterval + "}";
    }

    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        // Memory overhead for an object in a 64-bit JVM
        final int OBJECT_OVERHEAD = 16;
        // Memory overhead for an array in a 64-bit JVM
        final int ARRAY_OVERHEAD = 24;
        // Memory usage of int in a 64-bit JVM
        final int INT_SIZE = 4;
        // Memory usage of long in a 64-bit JVM
        final int LONG_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 8;
        
        // Base object size (object header + instance fields)
        long baseSize = OBJECT_OVERHEAD;
        baseSize += INT_SIZE;  // measure
        baseSize += INT_SIZE;  // count
        baseSize += REF_SIZE;  // aggregates reference
        baseSize += LONG_SIZE; // from
        baseSize += LONG_SIZE; // to
        baseSize += INT_SIZE;  // size
        baseSize += REF_SIZE;  // aggregateInterval reference
        
        // Size of the aggregates array 
        // (array overhead + size of each long element)
        long aggregatesSize = ARRAY_OVERHEAD + (aggregates.length * LONG_SIZE);
        
        // Size of AggregateInterval object (approximate)
        long aggregateIntervalSize = OBJECT_OVERHEAD + LONG_SIZE + REF_SIZE;
        
        // Total size
        return baseSize + aggregatesSize + aggregateIntervalSize;
    }

    public static int getAggSize(){
        return AGG_SIZE;

    }
    @Override
    public int getCount() {
        return count;
    }

    @Override
    public int getMeasure() { return measure; }

    /**
     * Rolls up this span to a coarser granularity by aggregating data points.
     * This method creates a new span with a larger aggregation interval.
     *
     * @param targetInterval The target aggregation interval to roll up to
     * @return A new span with rolled-up data, or null if the operation isn't possible
     */
    public M4InfAggregateTimeSeriesSpan rollUp(AggregateInterval targetInterval) {
        // Check if the target interval is coarser than the current one
        if (targetInterval.toDuration().toMillis() <= aggregateInterval.toDuration().toMillis()) {
            LOG.warn("Cannot roll up to a finer or equal granularity interval");
            return null;
        }
        
        // Check if the target interval is divisible by the current one
        long currentMs = aggregateInterval.toDuration().toMillis();
        long targetMs = targetInterval.toDuration().toMillis();
        
        // For simplicity, we'll require an even multiple, though this could be relaxed
        if (targetMs % currentMs != 0) {
            LOG.warn("Target interval must be an even multiple of the current interval");
            return null;
        }
        
        int factor = (int) (targetMs / currentMs);
        
        // Create new span
        M4InfAggregateTimeSeriesSpan rolledUpSpan = new M4InfAggregateTimeSeriesSpan(from, to, measure, targetInterval);
        
        // Process groups of indices to aggregate them together
        int newSize = (size + factor - 1) / factor; // ceil(size/factor)
        
        LOG.debug("Rolling up span from interval {} to {} (ratio 1:{}) - old size: {}, new size: {}",
                aggregateInterval, targetInterval, factor, size, newSize);
                
        // For each group of 'factor' intervals in the original span, compute aggregated values
        for (int newIdx = 0; newIdx < newSize; newIdx++) {
            final int startIdx = newIdx * factor;
            final int endIdx = Math.min((newIdx + 1) * factor, size);
            
            if (startIdx >= size) {
                break; // Shouldn't happen, but just to be safe
            }
            
            // Initialize with values from first interval in the group
            final double groupMinValue = Double.longBitsToDouble(aggregates[startIdx * AGG_SIZE]);
            final long groupMinTimestamp = aggregates[startIdx * AGG_SIZE + 1];
            final double groupMaxValue = Double.longBitsToDouble(aggregates[startIdx * AGG_SIZE + 2]);
            final long groupMaxTimestamp = aggregates[startIdx * AGG_SIZE + 3];
            final double groupFirstValue = Double.longBitsToDouble(aggregates[startIdx * AGG_SIZE + 4]);
            
            // Get first timestamp from the first interval in the group
            final long groupFirstTimestamp = aggregates[startIdx * AGG_SIZE + 6];
            
            // For last value and timestamp, use the last interval in the group
            final int lastIntervalIdx = endIdx - 1;
            final double groupLastValue = Double.longBitsToDouble(aggregates[lastIntervalIdx * AGG_SIZE + 5]);
            final long groupLastTimestamp = aggregates[lastIntervalIdx * AGG_SIZE + 7];
            
            // Calculate total count for this group
            int groupTotalCount = 0;
            for (int i = startIdx; i < endIdx; i++) {
                groupTotalCount += (int) aggregates[i * AGG_SIZE + 8];
            }
            final int totalCount = groupTotalCount;
            
            // Find min/max values across all intervals in the group
            double minValue = groupMinValue;
            long minTimestamp = groupMinTimestamp;
            double maxValue = groupMaxValue;
            long maxTimestamp = groupMaxTimestamp;
            
            for (int i = startIdx + 1; i < endIdx; i++) {
                double currentMin = Double.longBitsToDouble(aggregates[i * AGG_SIZE]);
                long currentMinTs = aggregates[i * AGG_SIZE + 1];
                double currentMax = Double.longBitsToDouble(aggregates[i * AGG_SIZE + 2]);
                long currentMaxTs = aggregates[i * AGG_SIZE + 3];
                
                // Update min/max if needed
                if (currentMin < minValue) {
                    minValue = currentMin;
                    minTimestamp = currentMinTs;
                }
                
                if (currentMax > maxValue) {
                    maxValue = currentMax;
                    maxTimestamp = currentMaxTs;
                }
            }
            
            final double finalMinValue = minValue;
            final long finalMinTimestamp = minTimestamp;
            final double finalMaxValue = maxValue;
            final long finalMaxTimestamp = maxTimestamp;
            final long finalFirstTimestamp = groupFirstTimestamp;
            final long finalLastTimestamp = groupLastTimestamp;
            
            // Calculate the timestamp for this new aggregated interval
            final long timestamp = from + (newIdx * targetInterval.toDuration().toMillis());
            
            // Create a stats object for the aggregation
            Stats aggStats = new Stats() {
                @Override
                public int getCount() { return totalCount; }
                @Override
                public double getMinValue() { return finalMinValue; }
                @Override
                public double getMaxValue() { return finalMaxValue; }
                @Override
                public long getMinTimestamp() { return finalMinTimestamp; }
                @Override
                public long getMaxTimestamp() { return finalMaxTimestamp; }
                @Override
                public double getFirstValue() { return groupFirstValue; }
                @Override
                public long getFirstTimestamp() { return finalFirstTimestamp; }
                @Override
                public double getLastValue() { return groupLastValue; }
                @Override
                public long getLastTimestamp() { return finalLastTimestamp; }
            };
            
            // Create an aggregated data point and add it to the new span
            AggregatedDataPoint aggPoint = new AggregatedDataPoint() {
                @Override
                public int getMeasure() { return measure; }
                @Override
                public long getTimestamp() { return timestamp; }
                @Override
                public Stats getStats() { return aggStats; }
                @Override
                public long getFrom() { return timestamp; }
                @Override
                public long getTo() { return timestamp + targetInterval.toDuration().toMillis(); }
                @Override
                public double getValue() { throw new UnsupportedOperationException(); }
                @Override
                public int getCount() { return totalCount; }
            };
            
            rolledUpSpan.addAggregatedDataPoint(aggPoint);
        }
        
        return rolledUpSpan;
    }
    
    /**
     * Estimates the memory size reduction if this span is rolled up to the target interval.
     * 
     * @param targetInterval The target aggregation interval
     * @return Approximate memory size reduction in bytes (negative if it would increase)
     */
    public long estimateRollUpMemorySavings(AggregateInterval targetInterval) {
        long currentMs = aggregateInterval.toDuration().toMillis();
        long targetMs = targetInterval.toDuration().toMillis();
        
        if (targetMs <= currentMs) {
            return -1; // Can't roll up to a finer granularity
        }
        
        long currentSize = calculateDeepMemorySize();
        long factor = targetMs / currentMs;
        int newArraySize = (int)((size + factor - 1) / factor); // ceil(size/factor)
        
        // Most of the memory is in the aggregates array
        long newAggregatesSize = 24 + (newArraySize * AGG_SIZE * 8); // array overhead + elements
        
        // The rest of the fields are roughly the same
        long baseSize = currentSize - (24 + (size * AGG_SIZE * 8));
        
        long newSize = baseSize + newAggregatesSize;
        return currentSize - newSize;
    }

    private class TimeSeriesSpanIterator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {
        
        private Iterator<Integer> internalIt;

        private long timestamp;

        private int currentIndex = -1;

        public TimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp - 1) + 1 : size)
                    .iterator();
        }

        @Override
        public boolean hasNext() {
            return internalIt.hasNext();
        }

        @Override
        public AggregatedDataPoint next() {
            currentIndex = internalIt.next();
            timestamp = from + currentIndex * aggregateInterval.toDuration().toMillis();
            return this;
        }

        @Override
        public int getCount() {
            return (int) aggregates[currentIndex * AGG_SIZE + 8];
        }

        @Override
        public Stats getStats() {
            return new Stats() {

                private int index = currentIndex;

                @Override
                public int getCount() {
                    // Debug log to see what we're actually getting
                    int count = (int) aggregates[index * AGG_SIZE + 8];
                    return count;
                }

                @Override
                public double getMinValue() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE]);
                }

                @Override
                public double getMaxValue() {
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 2]);
                }

                @Override
                public long getMinTimestamp() {
                    return aggregates[index * AGG_SIZE + 1];
                }

                @Override
                public long getMaxTimestamp() {
                    return aggregates[index * AGG_SIZE + 3];
                }

                @Override
                public double getFirstValue() {
                    // return (getMinValue() + getMaxValue()) / 2;
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 4]);
                }

                @Override
                public long getFirstTimestamp() {
                    return aggregates[index * AGG_SIZE + 6];
                }

                @Override
                public double getLastValue() {
                    // return (getMinValue() + getMaxValue()) / 2;
                    return Double.longBitsToDouble(aggregates[index * AGG_SIZE + 5]);
                }

                @Override
                public long getLastTimestamp() {
                    return aggregates[index * AGG_SIZE + 7];
                }
            };
        }

        @Override
        public int getMeasure() {
            return measure;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public long getFrom() {
            return timestamp;
        }

        @Override
        public long getTo() {
            if (currentIndex == size - 1) {
                return to;
            } else {
                return from + (currentIndex + 1) * aggregateInterval.toDuration().toMillis();
            }
        }

        @Override
        public double getValue() {
            throw new UnsupportedOperationException();
        }
    }


}
