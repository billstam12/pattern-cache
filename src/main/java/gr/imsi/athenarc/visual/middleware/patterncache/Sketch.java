package gr.imsi.athenarc.visual.middleware.patterncache;


import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class Sketch implements TimeInterval  {


    private final long from;
    private final long to;

    private StatsAggregator statsAggregator = new StatsAggregator();

    public Sketch(long from, long to) {
        this.from = from;
        this.to = to;
    }  

    
    @Override
    public long getFrom() {
        return from;
    }
    @Override
    public long getTo() {
        return to;
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        Stats stats = dp.getStats();
        if (stats.getCount() > 0){
            statsAggregator.accept(dp);
        }
    }

    public String toString() {
        return "Sketch from " + from + " to " + to;
    }
}
