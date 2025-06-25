package gr.imsi.athenarc.middleware.domain;

import java.io.Serializable;

/**
 * An implementation of the Stats interface that calculates stats within a fixed interval [from, to).
 * This class only takes into account values and not timestamps of data points.
 */
public class AggregateStats implements Stats, Serializable {

    private long from;
    private long to;

    private Double minValue;
    private Double maxValue;
    private Double firstValue;
    private Double lastValue;

    public AggregateStats() {

    }

    @Override
    public long getMinTimestamp() {
        throw new UnsupportedOperationException("getMinTimestamp() is not supported for NonTimestampedStats");
    }

    @Override
    public long getMaxTimestamp() {
        throw new UnsupportedOperationException("getMinTimestamp() is not supported for NonTimestampedStats");
    }

    @Override
    public long getFirstTimestamp() {
        throw new UnsupportedOperationException("getMinTimestamp() is not supported for NonTimestampedStats");
    }

    @Override
    public long getLastTimestamp() {
        throw new UnsupportedOperationException("getMinTimestamp() is not supported for NonTimestampedStats");
    }


    @Override
    public double getMinValue() {
        if(minValue == null) {
            throw new IllegalStateException("minValue is not set");
        }
        return minValue;
    }
 
    @Override
    public double getMaxValue() {
        if(maxValue == null) {
            throw new IllegalStateException("maxValue is not set");
        }
        return maxValue;
    }
    
    @Override
    public double getFirstValue() {
        if(firstValue == null) {
            throw new IllegalStateException("firstValue is not set");
        }
        return firstValue;
    }

    @Override
    public double getLastValue() {
        if(lastValue == null) {
            throw new IllegalStateException("lastValue is not set");
        }
        return lastValue;
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
