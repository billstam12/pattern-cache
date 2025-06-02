package gr.imsi.athenarc.middleware.domain;

/**
 * A representation of statistics required to compute the OLS slope for multi-variate time series data points.
 */
public class SlopeStats implements Stats {
    
    private double sumX;
    private double sumY;
    private double sumXY;
    private double sumX2;
    private int count;
    
    public SlopeStats(double sumX, double sumY, double sumXY, double sumX2, int count) {
        this.sumX = sumX;
        this.sumY = sumY;
        this.sumXY = sumXY;
        this.sumX2 = sumX2;
        this.count = count;
    }
    
    public SlopeStats() {    }

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
}
