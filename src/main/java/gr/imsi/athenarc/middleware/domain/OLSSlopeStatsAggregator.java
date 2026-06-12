package gr.imsi.athenarc.middleware.domain;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * A representation of statistics required to compute the OLS slope for multi-variate time series data points.
 */
public class OLSSlopeStatsAggregator implements Consumer<OLSSlopeStats>, Serializable {
    
    private double sumX = 0.0;
    private double sumY = 0.0;
    private double sumXY = 0.0;
    private double sumX2 = 0.0;
    private int count = 0;

    
    public OLSSlopeStatsAggregator() {    }

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
    public void accept(OLSSlopeStats t) {
        if (t == null) {
            throw new IllegalArgumentException("Cannot accept null SlopeStats");
        }
        this.sumX += t.getSumX();
        this.sumY += t.getSumY();
        this.sumXY += t.getSumXY();
        this.sumX2 += t.getSumX2();
        this.count += t.getCount();
    }

    public void combine(OLSSlopeStatsAggregator slopeStatsAggregator) {
        if (slopeStatsAggregator == null) {
            throw new IllegalArgumentException("Cannot combine with null SlopeStatsAggregator");
        }
        this.sumX += slopeStatsAggregator.getSumX();
        this.sumY += slopeStatsAggregator.getSumY();
        this.sumXY += slopeStatsAggregator.getSumXY();
        this.sumX2 += slopeStatsAggregator.getSumX2();
        this.count += slopeStatsAggregator.getCount();
    }

    public OLSSlopeStatsAggregator clone(){
        OLSSlopeStatsAggregator clone = new OLSSlopeStatsAggregator();
        clone.sumX = this.sumX;
        clone.sumY = this.sumY;
        clone.sumXY = this.sumXY;
        clone.sumX2 = this.sumX2;
        clone.count = this.count;
        return clone;
    }
}
