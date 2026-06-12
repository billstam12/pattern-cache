package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * A {@link DataPoints} implementation that stores  a series of consecutive
 * raw data points.
 */
public class RawTimeSeriesSpan implements TimeSeriesSpan {
    private static final Logger LOG = LoggerFactory.getLogger(RawTimeSeriesSpan.class);

    int count = 0;
    /**
     * The raw datapoint values.
     */
    private double[] values;

    /**
     * The timestamps of the raw datapoints.
     */
    private long[] timestamps;

    // The start time value of the span
    private long from;

    // The end time value of the span
    // Keep in mind that the end time is not included in the span,
    private long to;

    /*
     The measure for which this span is for.
     */
    private int measure;

    protected RawTimeSeriesSpan(long from, long to, int measure) {
        this.from = from;
        this.to = to;
        this.measure = measure;
    }

    /**
     * @param dataPoints
     */


     protected void build(List<DataPoint> dataPoints) {
        // The caller collects into a list first because the raw sampling interval
        // is only approximate, so the point count isn't known until the list is
        // full. By this point it is — size the primitive arrays exactly and fill
        // in one pass, with no Double/Long boxing.
        count = dataPoints.size();
        values = new double[count];
        timestamps = new long[count];
        int i = 0;
        for (DataPoint dataPoint : dataPoints) {
            timestamps[i] = dataPoint.getTimestamp();
            values[i] = dataPoint.getValue();
            i++;
        }
    }


    /**
     * Finds the index in the span in which the given timestamp should be.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = Arrays.binarySearch(timestamps, timestamp);
        if (index < 0) {
            // If not exact match, convert negative index to insertion point
            index = -(index + 1);
        }
        return index;
    }

    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }

    @Override
    public AggregateInterval getAggregateInterval() {
        return AggregateInterval.of(1, ChronoUnit.MILLIS);
    }

    public Iterator<DataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new RawTimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }

    @Override
    public Iterator<DataPoint> iterator() {
        // Use the first and last timestamps as the range for the iterator
        return new RawTimeSeriesSpanIterator(from, to);
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
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(getFrom()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String toString() {
        return getFromDate() + " - " + getToDate() + " for measure: " + measure;
    }

    @Override
    public int getCount() {
        return count;
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
        // Memory usage of double in a 64-bit JVM
        final int DOUBLE_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 8;
        
        // Base object size (object header + instance fields)
        long baseSize = OBJECT_OVERHEAD;
        baseSize += INT_SIZE;  // count
        baseSize += REF_SIZE;  // values reference
        baseSize += REF_SIZE;  // timestamps reference
        baseSize += LONG_SIZE; // from
        baseSize += LONG_SIZE; // to
        baseSize += INT_SIZE;  // measure
        
        // Size of the values array 
        // (array overhead + size of each double element)
        long valuesSize = 0;
        if (values != null) {
            valuesSize = ARRAY_OVERHEAD + (values.length * DOUBLE_SIZE);
        }
        
        // Size of the timestamps array 
        // (array overhead + size of each long element)
        long timestampsSize = 0;
        if (timestamps != null) {
            timestampsSize = ARRAY_OVERHEAD + (timestamps.length * LONG_SIZE);
        }
        
        // Total size
        return baseSize + valuesSize + timestampsSize;
    }

    @Override
    public int getMeasure() {
        return measure;
    }

    private class RawTimeSeriesSpanIterator implements Iterator<DataPoint>, DataPoint {
        private final int endIndex;
        private int currentIndex;
        private long timestamp;
        private double value;

        public RawTimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            this.currentIndex = getIndex(queryStartTimestamp);
            this.endIndex = getIndex(queryEndTimestamp);
        }

        @Override
        public boolean hasNext() {
            return currentIndex < endIndex;
        }

        @Override
        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            timestamp = timestamps[currentIndex];
            value = values[currentIndex];
            currentIndex++;
            return this;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public double getValue() {
            return value;
        }

        @Override
        public int getMeasure() {
            return measure;
        }
    }
    
     public boolean isInit() {
        return false;
    }


    @Override
    public int getSize() {
        return count;
    }

     @Override
     public TimeSeriesSpan rollUp(AggregateInterval targetInterval) {
        
        throw new UnsupportedOperationException("Roll up not supported for non-aggregate span");
     }

     @Override
     public void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint) {
        throw new UnsupportedOperationException("RawTimeSeries span does not support 'addAggregatedDataPoint'");
     }

}
