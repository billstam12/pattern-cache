package gr.imsi.athenarc.middleware.query.pattern;

import java.io.Serializable;
import java.util.List;

import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.sketch.Sketch;

public class PatternQueryResults implements QueryResults, Serializable {

    List<List<List<Sketch>>> matches;
    long executionTime;
    
    public void setMatches(List<List<List<Sketch>>> matches) {
        this.matches = matches;
    }   

    public List<List<List<Sketch>>> getMatches() {
        return matches;
    }
    
    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public long getExecutionTime() {
        return executionTime;
    }
}
