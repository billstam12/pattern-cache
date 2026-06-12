package gr.imsi.athenarc.middleware.pattern.nfa;

import java.util.List;

import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.Sketch;

public class MatchResult {
    private final int consumedCount;
    private final List<Sketch> matchedSketches;
    private final ValueFilter valueFilter;

    public MatchResult(int consumedCount, List<Sketch> matchedSketches) {
        this(consumedCount, matchedSketches, null);
    }

    public MatchResult(int consumedCount, List<Sketch> matchedSketches, ValueFilter valueFilter) {
        this.consumedCount = consumedCount;
        this.matchedSketches = matchedSketches;
        this.valueFilter = valueFilter;
    }

    public int getConsumedCount() {
        return consumedCount;
    }

    public List<Sketch> getMatchedSketches() {
        return matchedSketches;
    }

    /**
     * Filter the segment was validated against. Null for epsilon transitions
     * (which do not consume any sketches and do not contribute to the match path).
     */
    public ValueFilter getValueFilter() {
        return valueFilter;
    }
}
