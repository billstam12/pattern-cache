package gr.imsi.athenarc.middleware.query.pattern;

import java.io.Serializable;
import java.util.List;

import gr.imsi.athenarc.middleware.pattern.PatternMatch;
import gr.imsi.athenarc.middleware.query.QueryResults;

public class PatternQueryResults implements QueryResults, Serializable {

    List<PatternMatch> matches;
    long executionTime;
    double cacheHitRatio = 0.0;
    long ioCount = 0;
    
    public void setMatches(List<PatternMatch> matches) {
        this.matches = matches;
    }   

    public List<PatternMatch> getMatches() {
        return matches;
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
}
