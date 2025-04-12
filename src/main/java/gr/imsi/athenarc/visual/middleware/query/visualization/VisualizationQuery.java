package gr.imsi.athenarc.visual.middleware.query.visualization;

import gr.imsi.athenarc.visual.middleware.query.Query;
import gr.imsi.athenarc.visual.middleware.query.QueryType;
import gr.imsi.athenarc.visual.middleware.domain.AggregateInterval;

public class VisualizationQuery implements Query {
    private long from;
    private long to;
    private int[] measures;
    private AggregateInterval aggregateInterval;
    
    public VisualizationQuery(long from, long to, int[] measures, AggregateInterval aggregateInterval) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.aggregateInterval = aggregateInterval;
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
    public int[] getMeasures() {
        return measures;
    }

    @Override
    public QueryType getType() {
        return QueryType.VISUALIZATION;
    }
    
    public AggregateInterval getAggregateInterval() {
        return aggregateInterval;
    }
}
