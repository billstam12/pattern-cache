package gr.imsi.athenarc.middleware.query.pattern;

import java.io.Serializable;
import java.util.List;

import gr.imsi.athenarc.middleware.pattern.PatternMatch;
import gr.imsi.athenarc.middleware.query.QueryResults;

public class PatternQueryResults implements QueryResults, Serializable {

    List<PatternMatch> matches;
    // Snapshot of what the approxOls path would have returned, captured before
    // the fallback overwrites finalConfident. When fallback doesn't fire this
    // is the same list as `matches` (so downstream scoring can treat the
    // approxOls path uniformly across all queries).
    List<PatternMatch> candidateMatches;
    long executionTime;
    double cacheHitRatio = 0.0;
    long ioCount = 0;

    // Per-query refinement stats — populated by the cached approxOls path; left at
    // defaults (no refinement, zero error) for the OLS pass-through and the no-cache
    // path so downstream consumers can treat the column uniformly.
    int initialAggFactor = 0;
    int finalAggFactor = 0;
    boolean refinementTriggered = false;
    boolean fallbackTriggered = false;
    double errorBefore = 0.0;
    double errorAfter = 0.0;
    int ambiguousAfter = 0;

    public void setMatches(List<PatternMatch> matches) {
        this.matches = matches;
    }

    public List<PatternMatch> getMatches() {
        return matches;
    }

    public void setCandidateMatches(List<PatternMatch> candidateMatches) {
        this.candidateMatches = candidateMatches;
    }

    public List<PatternMatch> getCandidateMatches() {
        return candidateMatches;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setCacheHitRatio(double cacheHitRatio) {
        this.cacheHitRatio = cacheHitRatio;
    }
    public void setIoCount(long ioCount) {
        this.ioCount = ioCount;
    }

    @Override
    public double getCacheHitRatio() {
        return cacheHitRatio;
    }

    @Override
    public long getIoCount() {
        return ioCount;
    }

    public int getInitialAggFactor() { return initialAggFactor; }
    public void setInitialAggFactor(int v) { this.initialAggFactor = v; }
    public int getFinalAggFactor() { return finalAggFactor; }
    public void setFinalAggFactor(int v) { this.finalAggFactor = v; }
    public boolean isRefinementTriggered() { return refinementTriggered; }
    public void setRefinementTriggered(boolean v) { this.refinementTriggered = v; }
    public boolean isFallbackTriggered() { return fallbackTriggered; }
    public void setFallbackTriggered(boolean v) { this.fallbackTriggered = v; }
    public double getErrorBefore() { return errorBefore; }
    public void setErrorBefore(double v) { this.errorBefore = v; }
    public double getErrorAfter() { return errorAfter; }
    public void setErrorAfter(double v) { this.errorAfter = v; }
    public int getAmbiguousAfter() { return ambiguousAfter; }
    public void setAmbiguousAfter(int v) { this.ambiguousAfter = v; }
}
