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

    /**
     * Sum of the raw values in this bucket. Optional capability: implementations
     * backed by a synopsis without a sum aggregate throw
     * {@link UnsupportedOperationException}. Sum is the canonical aggregate because
     * it combines additively across buckets — {@link #getMean()} is derived as
     * sum / count.
     */
    default double getSum() {
        throw new UnsupportedOperationException("getSum() not supported by " + getClass().getName());
    }

    /**
     * Mean of the raw values in this bucket, derived as {@link #getSum()} / {@link #getCount()}.
     * Available whenever the underlying synopsis carries sum; throws via {@link #getSum()}
     * when it does not. Returns {@code NaN} for empty buckets.
     */
    default double getMean() {
        int count = getCount();
        if (count <= 0) {
            return Double.NaN;
        }
        return getSum() / count;
    }

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
