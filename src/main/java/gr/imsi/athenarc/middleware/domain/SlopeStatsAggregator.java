package gr.imsi.athenarc.middleware.domain;

import java.io.Serializable;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SlopeStatsAggregator implements Consumer<AggregatedDataPoint>, Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SlopeStatsAggregator.class);
    private static final long serialVersionUID = 1L;

    protected int count;
    protected double firstValue;
    protected long firstTimestamp;
    protected double lastValue;
    protected long lastTimestamp;

    public SlopeStatsAggregator() {
        clear();
    }

    public void clear() {
        count = 0;
        firstTimestamp =  Long.MAX_VALUE;
        lastTimestamp = -1L;
    }

    @Override
    public void accept(AggregatedDataPoint dataPoint) {
        if (dataPoint == null || dataPoint.getStats() == null) {
            LOG.warn("Null aggregated data point or stats encountered, skipping");
            return;
        }
        
        Stats stats = dataPoint.getStats();
        if (dataPoint.getCount() > 0) {
            if (firstTimestamp > stats.getFirstTimestamp()) {
                firstValue = stats.getFirstValue();
                firstTimestamp = stats.getFirstTimestamp();
            }
            if (lastTimestamp < stats.getLastTimestamp()) {
                lastValue = stats.getLastValue();
                lastTimestamp = stats.getLastTimestamp();
            }
            count += dataPoint.getCount();
        }
    }

    
    /**
     * Combines the state of a {@code Stats} instance into this
     * StatsAggregator.
     *
     * @param other another {@code Stats}
     * @throws IllegalArgumentException if the other Stats instance does not have the same measures as this StatsAggregator
     */
    public void combine(Stats other) {
        if (other == null) {
            LOG.warn("Null stats encountered in combine operation, skipping");
            return;
        }
        
        if(other.getCount() > 0) {
            if (firstTimestamp > other.getFirstTimestamp()) {
                firstValue = other.getFirstValue();
                firstTimestamp = other.getFirstTimestamp();
            }
            if (lastTimestamp < other.getLastTimestamp()) {
                lastValue = other.getLastValue();
                lastTimestamp = other.getLastTimestamp();
            }
            count += other.getCount();
        }
    }


    @Override
    public int getCount() {
        return count;
    }


    @Override
    public double getFirstValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstValue;
    }

    @Override
    public long getFirstTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstTimestamp;
    }

    @Override
    public double getLastValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastValue;
    }

    @Override
    public long getLastTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastTimestamp;
    }

    public SlopeStatsAggregator clone() {
        SlopeStatsAggregator statsAggregator = new SlopeStatsAggregator();
        statsAggregator.combine(this);
        return statsAggregator;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        if (count == 0) {
            return "No data points";
        }
        return "Stats{count=" + count + 
               ", first=" + firstValue +
               ", last=" + lastValue + "}";
    }

    @Override
    public double getMinValue() {
        throw new UnsupportedOperationException("Unimplemented method 'getMinValue'");
    }

    @Override
    public long getMinTimestamp() {
        throw new UnsupportedOperationException("Unimplemented method 'getMinTimestamp'");
    }

    @Override
    public double getMaxValue() {
        throw new UnsupportedOperationException("Unimplemented method 'getMaxValue'");
    }

    @Override
    public long getMaxTimestamp() {
        throw new UnsupportedOperationException("Unimplemented method 'getMaxTimestamp'");
    }
}
