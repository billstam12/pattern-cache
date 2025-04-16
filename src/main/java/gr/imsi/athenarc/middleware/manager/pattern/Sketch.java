package gr.imsi.athenarc.middleware.manager.pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;

public class Sketch implements AggregatedDataPoint {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

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
    
    public void combine(Sketch sketch) {
        if (this.getTo() != sketch.getFrom()) {
            throw new IllegalArgumentException("Cannot combine sketches that are not consecutive. " +
                "Current sketch ends at " + this.getTo() + " but next sketch starts at " + sketch.getFrom());
        }
        this.to = sketch.getTo();
        this.statsAggregator.combine(sketch.getStats());
        this.duration += 1;
    }

    public double computeSlope() {
        Stats stats = this.getStats();

        if(stats.getCount() == 0){
            return 0;
        }
        
        // Check if there are enough data points to calculate slope
        if (this.getDuration() < 1) {
            throw new IllegalArgumentException("Cannot compute slope with fewer than 2 data points. Found sketch with duration: " + this.getDuration());
        }
                
        // Calculate slope based on first and last data points
        double valueChange = stats.getLastValue() - stats.getFirstValue();
        
        double slope = valueChange / (this.getDuration() - 1);
        
        // Using a simple normalization approach - could be refined based on expected slope ranges
        double normalizedSlope = Math.atan(slope) / Math.PI; // Maps to range [-0.5,0.5]
        
        return normalizedSlope;
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


    public DataPoint getRepresentativeDataPoint() {
        if(statsAggregator.getCount() == 0){
            return null;
        } 
        return new ImmutableDataPoint(statsAggregator.getLastTimestamp(), statsAggregator.getLastValue(), this.getMeasure());
    }

    @Override
    public long getTimestamp() {
        return getRepresentativeDataPoint().getTimestamp();   
    }

    @Override
    public double getValue() {
        if(statsAggregator.getCount() == 0){
            return 0;
        } 
        return getRepresentativeDataPoint().getValue(); 
    }

    @Override
    public int getCount() {
       return statsAggregator.getCount();
    }

    @Override
    public Stats getStats() {
       return statsAggregator;
    }

    public String toString() {
        return "Sketch from " + from + " to " + to + " duration " + duration + " slope: " + computeSlope();
    }

    @Override
    public int getMeasure(){
        return -1;
    }


}
