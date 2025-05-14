package gr.imsi.athenarc.middleware.domain;

/**
 * A representation of aggregate statistics for multi-variate time series data points.
 */
public interface Stats {
    
    public int getCount();

    public double getMinValue();

    public long getMinTimestamp();

    public double getMaxValue();

    public long getMaxTimestamp();

    public double getFirstValue();

    public long getFirstTimestamp();

    public double getLastValue();

    public long getLastTimestamp();

    default DataPoint getMinDataPoint() {
        return new ImmutableDataPoint(getMinTimestamp(), getMinValue(), -1);
    }

    default DataPoint getMaxDataPoint() {
        return new ImmutableDataPoint(getMaxTimestamp(), getMaxValue(), -1);
    }

    default DataPoint getFirstDataPoint() {
        return new ImmutableDataPoint(getFirstTimestamp(), getFirstValue(), -1);
    }

    default DataPoint getLastDataPoint() {
        return new ImmutableDataPoint(getLastTimestamp(), getLastValue(), -1);
    }

    default String toString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", min=" + getMinValue() +
                ", minTimestamp=" + getMinTimestamp() +
                ", max=" + getMaxValue() +
                ", maxTimestamp=" + getMaxTimestamp() +
                ", first=" + getFirstValue() +
                ", firstTimestamp=" + getFirstTimestamp() +
                ", last=" + getLastValue() +
                ", lastTimestamp=" + getLastTimestamp() +
                '}';
    }

}
