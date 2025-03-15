package gr.imsi.athenarc.visual.middleware.patterncache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;

/**
 * Demonstrates a BFS (NFA-like) approach to match all segments of a PatternQuery
 * against a list of Sketches. Collects all possible matches.
 */
public class SketchSearchBFS {
    
    private static final Logger LOG = LoggerFactory.getLogger(SketchSearchBFS.class);
    
    private final List<Sketch> sketches;
    private final List<PatternNode> patternNodes;
    
    // We'll store all complete matches here.
    // Each match is a List<List<Sketch>> (one sub-list for each segment).
    private final List<List<List<Sketch>>> allMatches = new ArrayList<>();
    
    public SketchSearchBFS(List<Sketch> sketches, List<PatternNode> patternNodes) {
        this.sketches = sketches;
        this.patternNodes = patternNodes;
    }
    
    /**
     * Runs the BFS search, collects *all* matches (not just the first).
     * Returns the list of matches, where each match is a list-of-lists-of-Sketch.
     */
    public List<List<List<Sketch>>> findAllMatches() {
        
        // Basic checks
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            LOG.warn("Either the sketches list is empty or the pattern query has no segments.");
            return allMatches; // empty
        }
        
        // We do a BFS over states: (segmentIndex, startIndex, partialSolutionSoFar)
        class State {
            final int segmentIndex;
            final int startIndex;
            final List<List<Sketch>> partialSolution; 
            
            State(int segIdx, int stIdx, List<List<Sketch>> partial) {
                this.segmentIndex = segIdx;
                this.startIndex = stIdx;
                // We don't want to mutate the original partial, so copy it if needed
                this.partialSolution = partial;
            }

            public String toString() {
                return String.format("State{segIdx=%d, stIdx=%d, partial=%s}", segmentIndex, startIndex, partialSolution);
            }
        }
        
        Queue<State> queue = new LinkedList<>();
        
        int totalSegments = patternNodes.size();
        
        // Initialize the queue: we can start from any index in sketches
        for (int startIdx = 0; startIdx < sketches.size(); startIdx++) {
            List<List<Sketch>> emptyPartial = new ArrayList<>();
            queue.add(new State(0, startIdx, emptyPartial));
        }
        
        while (!queue.isEmpty()) {
            State current = queue.poll();
            int segIndex = current.segmentIndex;
            int startIdx = current.startIndex;
            List<List<Sketch>> partialSoFar = current.partialSolution;
            
            // If segIndex == totalSegments, that means we've matched ALL segments
            // partialSoFar is a complete match
            if (segIndex >= totalSegments) {
                // Store this complete match
                allMatches.add(partialSoFar);
                // We continue to see if there are more matches in the queue
                continue;
            }
            
            // If startIdx >= sketches.size(), we can't match further
            if (startIdx >= sketches.size()) {
                continue;
            }
            
            // Try matching the current segment at (segIndex) 
            SegmentSpecification segmentSpec = ((SegmentSpecNode) patternNodes.get(segIndex)).getSpec();
            
            // Get all possible subranges for this segment from "startIdx"
            List<List<Sketch>> possibleMatches = findPossibleMatches(startIdx, segmentSpec);
            for (List<Sketch> subrange : possibleMatches) {
                // Build the updated partial solution for the next state
                List<List<Sketch>> newPartial = new ArrayList<>(partialSoFar);
                newPartial.add(subrange); // matched subrange for this segment
                
                // The next segment starts after we consume 'subrange.size()' sketches
                int nextStartIndex = startIdx + subrange.size();
                
                // Move to the next segment
                State nextState = new State(segIndex + 1, nextStartIndex, newPartial);
                queue.add(nextState);
            }
        }
        
        return allMatches;
    }
    
    /**
     * Finds all possible ways to match a single segment at the given starting position,
     * returning a list of subranges (each subrange is a list of Sketch).
     */
    private List<List<Sketch>> findPossibleMatches(int startIndex, SegmentSpecification segment) {
        List<List<Sketch>> possibleMatches = new ArrayList<>();
        
        TimeFilter timeFilter = segment.getTimeFilter();
        ValueFilter valueFilter = segment.getValueFilter();
        
        int minSketches = Math.max(2, timeFilter.getTimeLow());
        int maxSketches = timeFilter.getTimeHigh() + 1;
                
        // We'll iterate from [minSketches..maxSketches], as long as we stay in range
        for (int count = minSketches; 
             count <= maxSketches && (startIndex + count) <= sketches.size(); 
             count++) {
            
            // Build one "composite" from sketches[startIndex .. startIndex+count-1]
            Sketch composite = new Sketch(sketches.get(startIndex).getFrom(),
                                          sketches.get(startIndex).getTo());
            List<Sketch> segmentSketches = new ArrayList<>();
            segmentSketches.add(sketches.get(startIndex));
            composite.addAggregatedDataPoint(sketches.get(startIndex));
            
            boolean combinationValid = true;
            for (int i = 1; i < count; i++) {
                Sketch nextSketch = sketches.get(startIndex + i);
                try {
                    composite.combine(nextSketch);
                    segmentSketches.add(nextSketch);
                } catch (Exception e) {
                    LOG.error("Failed to combine sketches at index {}: {}", startIndex + i, e.getMessage());
                    combinationValid = false;
                    break;
                }
            }
            
            if (!combinationValid) {
                // This combination fails, skip it
                continue;
            }
            
            // Ensure we have at least 2 sketches for slope
            if (count < 2) {
                continue;
            }
            
            // Now check slope / value constraints
            if (matchesComposite(composite, valueFilter)) {
                possibleMatches.add(segmentSketches);
            }
        }
        
        return possibleMatches;
    }
    
    /**
     * Computes the slope of a composite sketch against the ValueFilter of a segment.
     * Returns true if the slope is within the filter's range.
     */
    private boolean matchesComposite(Sketch sketch, ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double slope = sketch.computeSlope();
        double low = filter.getValueLow();
        double high = filter.getValueHigh();
        boolean match = (slope >= low && slope <= high);
        return match;
    }
    
}
