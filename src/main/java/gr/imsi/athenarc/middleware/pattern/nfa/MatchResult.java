package gr.imsi.athenarc.middleware.pattern.nfa;

import java.util.List;

import gr.imsi.athenarc.middleware.cache.Sketch;

public class MatchResult {
    private final int consumedCount;
    private final List<Sketch> matchedSketches;

    public MatchResult(int consumedCount, List<Sketch> matchedSketches) {
        this.consumedCount = consumedCount;
        this.matchedSketches = matchedSketches;
    }

    public int getConsumedCount() {
        return consumedCount;
    }

    public List<Sketch> getMatchedSketches() {
        return matchedSketches;
    }
}