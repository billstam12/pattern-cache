package gr.imsi.athenarc.middleware.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * An implementation of the Stats interface that calculates stats within a fixed interval [from, to).
 * This class only takes into account values and not timestamps of data points.
 * As timestamps the middle of the interval is used for the data points with the min and max values,
 * and the from and to timestamps for the first and last data points.
 */
public class NonTimestampedStats implements Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NonTimestampedStats.class);

    private long from;
    private long to;

    private Double minValue;
    private Double maxValue;
    private Double firstValue;
    private Double lastValue;

    public NonTimestampedStats() {
    }

    public double getMinValue() {
        return minValue;
    }

    @Override
    public long getMinTimestamp() {
        return (from + to) / 2;
    }

    public double getMaxValue() {
        return maxValue;
    }

    @Override
    public long getMaxTimestamp() {
        return (from + to) / 2;
    }

    @Override
    public double getFirstValue() {
        return firstValue == null ? (getMinValue() + getMaxValue()) / 2 : firstValue;
    }

    @Override
    public long getFirstTimestamp() {
        return from + 1;
    }

    @Override
    public double getLastValue() {
        return lastValue == null ? (getMinValue() + getMaxValue()) / 2 : lastValue;
    }

    @Override
    public long getLastTimestamp() {
        return to - 1;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public void setFirstValue(double firstValue) {
        this.firstValue = firstValue;
    }

    public void setLastValue(double lastValue) {
        this.lastValue = lastValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }   

    public int getCount() {
        int count = 0;
        if (firstValue != null) {
            count++;
        }
        if (lastValue != null) {
            count++;
        }
        if (minValue != null) {
            count++;
        }
        if (maxValue != null) {
            count++;
        }
        return count; 
    }

    @Override
    public String toString() {
        return "NonTimestampedStatsAggregator [from=" + from + ", to=" + to 
                + ", minValue=" + minValue + ", maxValue=" + maxValue + "]";
    }

    
}
