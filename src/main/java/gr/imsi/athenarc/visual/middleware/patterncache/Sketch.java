package gr.imsi.athenarc.visual.middleware.patterncache;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;

public class Sketch implements AggregatedDataPoint {

    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);

    private long from;
    private long to;

    private StatsAggregator statsAggregator = new StatsAggregator();

    private int duration;

    public Sketch(long from, long to) {
        this.from = from;
        this.to = to;
        this.duration = 1;
    }  
   
    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        Stats stats = dp.getStats();
        if (stats.getCount() > 0){
            statsAggregator.accept(dp);
        }
    }
    
    public void combine(Sketch sketch){
        if(this.getTo() == sketch.getFrom()){
            this.to = sketch.getTo();
            this.statsAggregator.combine(sketch.getStats());
            this.duration += sketch.getDuration();
        }
        else{
            LOG.error("Cannot combine sketches that are not consecutive");
        }
    }

    public int getDuration(){
        return duration;
    }

    @Override
    public long getFrom() {
        return from;
    }
    @Override
    public long getTo() {
        return to;
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException("Sketch does not have a representative timestamp");
    }

    @Override
    public double getValue() {
        if(statsAggregator.getCount() == 0){
            return 0;
        }  
        DataPoint firstDataPoint = statsAggregator.getFirstDataPoint();
        DataPoint lastDataPoint = statsAggregator.getLastDataPoint();
        
        // Calculate slope based on first and last data points
        double valueChange = lastDataPoint.getValue() - firstDataPoint.getValue();
        long timeChange = lastDataPoint.getTimestamp() - firstDataPoint.getTimestamp();
        
        if (timeChange == 0) {
            return 0; // No time difference, return flat slope
        }
        
        double slope = valueChange / timeChange;
        
        // Using a simple normalization approach - could be refined based on expected slope ranges
        double normalizedSlope = 0.5 + Math.atan(slope) / Math.PI; // Maps to range [-0.5,0.5]
        
        return normalizedSlope;
    }

    @Override
    public int getCount() {
       return statsAggregator.getCount();
    }

    @Override
    public Stats getStats() {
       return statsAggregator;
    }

    @Override
    public int getMeasure() {
        return -1;
    }

    public String toString() {
        return "Sketch from " + from + " to " + to;
    }


}
