package gr.imsi.athenarc.middleware.pattern;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

public interface Sketch extends TimeInterval { 
    
    public double getSlope();

    public AggregationType getAggregationType();

    public Sketch combine(Sketch other);

    public boolean isEmpty();

    public Sketch clone();

    public void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint);

    public boolean hasInitialized();
}
