package gr.imsi.athenarc.middleware.domain;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * A representation of statistics required to compute the OLS slope for multi-variate time series data points.
 */
public class SlopeStatsAggregator implements Consumer<SlopeStats>, Stats, Serializable {
    
    private double sumX;
    private double sumY;
    private double sumXY;
    private double sumX2;
    private int count;

    
    public SlopeStatsAggregator() {    }

    public double getSumX(){
        return sumX;
    }
    
    public double getSumY(){
        return sumY;
    }

    public double getSumXY(){
        return sumXY;
    }

    public double getSumX2(){
        return sumX2;
    }

    public int getCount(){
        return count;
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

    @Override
    public double getFirstValue() {
        throw new UnsupportedOperationException("Unimplemented method 'getFirstValue'");
    }

    @Override
    public long getFirstTimestamp() {
        throw new UnsupportedOperationException("Unimplemented method 'getFirstTimestamp'");
    }

    @Override
    public double getLastValue() {
        throw new UnsupportedOperationException("Unimplemented method 'getLastValue'");
    }

    @Override
    public long getLastTimestamp() {
        throw new UnsupportedOperationException("Unimplemented method 'getLastTimestamp'");
    }

    @Override
    public void accept(SlopeStats t) {
        if (t == null) {
            throw new IllegalArgumentException("Cannot accept null SlopeStats");
        }
        this.sumX += t.getSumX();
        this.sumY += t.getSumY();
        this.sumXY += t.getSumXY();
        this.sumX2 += t.getSumX2();
        this.count += t.getCount();
    }

    public void combine(SlopeStatsAggregator slopeStatsAggregator) {
        if (slopeStatsAggregator == null) {
            throw new IllegalArgumentException("Cannot combine with null SlopeStatsAggregator");
        }
        this.sumX += slopeStatsAggregator.getSumX();
        this.sumY += slopeStatsAggregator.getSumY();
        this.sumXY += slopeStatsAggregator.getSumXY();
        this.sumX2 += slopeStatsAggregator.getSumX2();
        this.count += slopeStatsAggregator.getCount();
    }

    public SlopeStatsAggregator clone(){
        SlopeStatsAggregator clone = new SlopeStatsAggregator();
        clone.sumX = this.sumX;
        clone.sumY = this.sumY;
        clone.sumXY = this.sumXY;
        clone.sumX2 = this.sumX2;
        clone.count = this.count;
        return clone;
    }
}
