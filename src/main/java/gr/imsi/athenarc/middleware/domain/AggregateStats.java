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
    /** Real raw-sample count for this bucket. -1 means "unset" so {@link #getCount()}
     *  falls back to the legacy fingerprint behaviour (number of non-null aggregates). */
    private int count = -1;

    public AggregateStats() {

    }

    @Override
    public long getMinTimestamp() {
        return ( from + to ) / 2; 
    }

    @Override
    public long getMaxTimestamp() {
        return ( from + to ) / 2; 
    }

    @Override
    public long getFirstTimestamp() {
        return from + 1;
    }

    @Override
    public long getLastTimestamp() {
        return to - 1;
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

    /** Set the real raw-sample count for this bucket (from the data source). */
    public void setCount(int count) {
        this.count = count;
    }

    /**
     * Real raw-sample count when it has been threaded through (e.g. {@code count(*)}
     * from the SQL aggregate). Falls back to a non-null-fields fingerprint so legacy
     * callers using {@code getCount() > 0} as a "has any data" sentinel keep working.
     */
    public int getCount() {
        if (count >= 0) {
            return count;
        }
        int fingerprint = 0;
        if (firstValue != null) fingerprint++;
        if (lastValue != null) fingerprint++;
        if (minValue != null) fingerprint++;
        if (maxValue != null) fingerprint++;
        return fingerprint;
    }

    @Override
    public String toString() {
        return "AggregateStats [from=" + from + ", to=" + to 
                + ", minValue=" + minValue + ", maxValue=" + maxValue + "]";
    }

    
}
