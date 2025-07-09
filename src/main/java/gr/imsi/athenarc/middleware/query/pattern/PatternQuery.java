package gr.imsi.athenarc.middleware.query.pattern;

import java.util.Arrays;
import java.util.List;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.SlopeFunction;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryType;

public class PatternQuery implements Query {
    private final long from;
    private final long to;
    private final int measure;
    private final AggregateInterval timeUnit;
    private final SlopeFunction slopeFunction;
    private final List<PatternNode> patternNodes;

    private final ViewPort viewPort;
    private final double accuracy;
    
    public PatternQuery(long from, long to, int measure, AggregateInterval timeUnit,
     SlopeFunction slopeFunction, List<PatternNode> patternNodes, int width, int height, double accuracy) {
        this.from = from;
        this.to = to;
        this.measure = measure;
        this.timeUnit = timeUnit;
        this.slopeFunction = slopeFunction;
        this.patternNodes = patternNodes;
        this.viewPort = new ViewPort(width, height);
        this.accuracy = accuracy;
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
    public List<Integer> getMeasures() {
        return Arrays.asList(measure);
    }

    @Override
    public QueryType getType() {
        return QueryType.PATTERN;
    }

    public AggregateInterval getTimeUnit() {
        return timeUnit;
    }

    public List<PatternNode> getPatternNodes() {
        return patternNodes;
    }

    public SlopeFunction getSlopeFunction() {
        return slopeFunction;
    }
    
    // For backward compatibility during transition
    public List<PatternNode> getSegmentSpecifications() {
        return patternNodes;
    }

    public ViewPort getViewPort(){
        return viewPort;
    }

    public double getAccuracy(){
        return accuracy;
    }
}
