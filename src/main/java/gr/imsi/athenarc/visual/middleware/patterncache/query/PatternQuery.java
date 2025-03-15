package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.time.temporal.ChronoUnit;
import java.util.List;

public class PatternQuery {
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

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public int getMeasure() {
        return measure;
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
