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
    
    public void combine(Sketch sketch) {
        if (this.getTo() != sketch.getFrom()) {
            throw new IllegalArgumentException("Cannot combine sketches that are not consecutive. " +
                "Current sketch ends at " + this.getTo() + " but next sketch starts at " + sketch.getFrom());
        }
        this.to = sketch.getTo();
        this.statsAggregator.combine(sketch.getStats());
        this.duration += 1;
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
    public DataPoint getRepresnentativeDataPoint() {
        if(statsAggregator.getCount() == 0){
            return null;
        } 
        return statsAggregator.getLastDataPoint();
    }

    @Override
    public long getTimestamp() {
        return getRepresnentativeDataPoint().getTimestamp();   
    }

    @Override
    public double getValue() {
        if(statsAggregator.getCount() == 0){
            return 0;
        } 
        return getRepresnentativeDataPoint().getValue(); 
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
