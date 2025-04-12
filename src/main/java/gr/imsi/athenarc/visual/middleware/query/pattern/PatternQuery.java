package gr.imsi.athenarc.visual.middleware.query.pattern;

import gr.imsi.athenarc.visual.middleware.query.Query;
import gr.imsi.athenarc.visual.middleware.query.QueryType;

import java.time.temporal.ChronoUnit;
import java.util.List;

public class PatternQuery implements Query {
    private final long from;
    private final long to;
    private final int measure;
    private final ChronoUnit chronoUnit;
    private final List<PatternNode> patternNodes;

    public PatternQuery(long from, long to, int measure, ChronoUnit chronoUnit, List<PatternNode> patternNodes) {
        this.from = from;
        this.to = to;
        this.measure = measure;
        this.chronoUnit = chronoUnit;
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

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    public List<PatternNode> getPatternNodes() {
        return patternNodes;
    }

    // For backward compatibility during transition
    public List<PatternNode> getSegmentSpecifications() {
        return patternNodes;
    }
}
