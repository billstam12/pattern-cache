package gr.imsi.athenarc.middleware.domain;

/**
 * Extension of OLSSlopeStats that also stores min and max values for a bucket.
 */
public class OLSSlopeMinMaxStats extends OLSSlopeStats {
    private final double minValue;
    private final double maxValue;

    public OLSSlopeMinMaxStats(double sumX, double sumY, double sumXY, double sumX2, int count, double minValue, double maxValue) {
        super(sumX, sumY, sumXY, sumX2, count);
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public double getMinValue() {
        return minValue;
    }

    @Override
    public double getMaxValue() {
        return maxValue;
    }
}
