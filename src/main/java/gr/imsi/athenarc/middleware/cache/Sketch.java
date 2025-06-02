package gr.imsi.athenarc.middleware.cache;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

public interface Sketch extends TimeInterval { 
    
    public double getAngle();

    public AggregationType getAggregationType();

    public Sketch combine(Sketch other);

    public boolean isEmpty();

    public Sketch clone();

    public void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint);

    public boolean hasInitialized();

    public boolean matches(ValueFilter filter);
}
