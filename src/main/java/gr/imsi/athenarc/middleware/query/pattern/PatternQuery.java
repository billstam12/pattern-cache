package gr.imsi.athenarc.middleware.query.pattern;

import java.util.List;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryType;

public class PatternQuery implements Query {
    private final long from;
    private final long to;
    private final int measure;
    private final AggregateInterval timeUnit;
    private final List<PatternNode> patternNodes;

    public PatternQuery(long from, long to, int measure, AggregateInterval timeUnit, List<PatternNode> patternNodes) {
        this.from = from;
        this.to = to;
        this.measure = measure;
        this.timeUnit = timeUnit;
        this.patternNodes = patternNodes;
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
        return new int[]{measure};
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

    // For backward compatibility during transition
    public List<PatternNode> getSegmentSpecifications() {
        return patternNodes;
    }
}
