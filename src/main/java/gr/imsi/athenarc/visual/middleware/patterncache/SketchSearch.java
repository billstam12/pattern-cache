package gr.imsi.athenarc.visual.middleware.patterncache;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.patterncache.query.GroupNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.RepetitionFactor;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;

/**
 * Demonstrates a recursive approach to match all segments of a PatternQuery
 * against a list of Sketches. Collects all possible matches.
 */
public class SketchSearch {
    
    private static final Logger LOG = LoggerFactory.getLogger(SketchSearch.class);
    
    private final List<Sketch> sketches;
    private final List<PatternNode> patternNodes;
    
    // We'll store all complete matches here.
    // Each match is a List<List<Sketch>> (one sub-list for each segment).
    private final List<List<List<Sketch>>> allMatches = new ArrayList<>();
    
    public SketchSearch(List<Sketch> sketches, List<PatternNode> patternNodes) {
        this.sketches = sketches;
        this.patternNodes = patternNodes;
    }
    
    /**
     * Runs the search, collects *all* matches (not just the first).
     * Returns the list of matches, where each match is a list-of-lists-of-Sketch.
     */
    public List<List<List<Sketch>>> findAllMatches() {
        
        // Basic checks
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            LOG.warn("Either the sketches list is empty or the pattern has no nodes.");
            return allMatches; // empty
        }
        
        // Start matching from any sketch position
        for (int startIdx = 0; startIdx < sketches.size(); startIdx++) {
            PatternMatcher matcher = new PatternMatcher(sketches);
            List<List<Sketch>> partialMatch = new ArrayList<>();
            matcher.matchPatternNodes(patternNodes, startIdx, partialMatch, allMatches);
        }
        
        return allMatches;
    }
    
    /**
     * Helper class to handle the recursive pattern matching logic
     */
    private class PatternMatcher {
        private final List<Sketch> sketches;
        
        public PatternMatcher(List<Sketch> sketches) {
            this.sketches = sketches;
        }
        
        /**
         * Recursively matches pattern nodes against sketches
         * 
         * @param nodes The pattern nodes to match
         * @param startIdx Starting index in the sketch list
         * @param partialMatch Current partial match being built
         * @param allMatches Collection of all complete matches
         */
        public void matchPatternNodes(List<PatternNode> nodes, int startIdx, 
                                      List<List<Sketch>> partialMatch, 
                                      List<List<List<Sketch>>> allMatches) {
            
            // If we've matched all nodes, we have a complete match
            if (nodes.isEmpty()) {
                allMatches.add(new ArrayList<>(partialMatch));
                return;
            }
            
            // If we've run out of sketches, we can't match further
            if (startIdx >= sketches.size()) {
                return;
            }
            
            PatternNode currentNode = nodes.get(0);
            List<PatternNode> remainingNodes = nodes.subList(1, nodes.size());
            
            // Handle node based on its type
            if (currentNode instanceof SegmentSpecNode) {
                matchSingleNode((SegmentSpecNode) currentNode, startIdx, remainingNodes, partialMatch, allMatches);
            } else if (currentNode instanceof GroupNode) {
                matchGroupNode((GroupNode) currentNode, startIdx, remainingNodes, partialMatch, allMatches);
            }
        }
        
        /**
         * Matches a SegmentSpecNode against sketches
         */
        private void matchSingleNode(SegmentSpecNode node, int startIdx, 
                                         List<PatternNode> remainingNodes, 
                                         List<List<Sketch>> partialMatch, 
                                         List<List<List<Sketch>>> allMatches) {
            
            SegmentSpecification spec = node.getSpec();
            RepetitionFactor repetition = node.getRepetitionFactor();
            int minReps = repetition.getMinRepetitions();
            int maxReps = repetition.getMaxRepetitions();
            
            // Get all ways to match a single occurrence of this segment
            List<List<Sketch>> possibleMatches = findPossibleMatches(startIdx, spec);
            
            // For each number of repetitions in the allowed range
            for (int reps = minReps; reps <= maxReps; reps++) {
                matchRepeatedSegment(node, possibleMatches, reps, startIdx, remainingNodes, 
                                    partialMatch, allMatches);
            }
        }
        
        /**
         * Matches a GroupNode against sketches
         */
        private void matchGroupNode(GroupNode node, int startIdx, 
                                   List<PatternNode> remainingNodes, 
                                   List<List<Sketch>> partialMatch, 
                                   List<List<List<Sketch>>> allMatches) {
            
            List<PatternNode> groupChildren = node.getChildren();
            RepetitionFactor repetition = node.getRepetitionFactor();
            int minReps = repetition.getMinRepetitions();
            int maxReps = repetition.getMaxRepetitions();
            
            // For each number of repetitions in the allowed range
            for (int reps = minReps; reps <= maxReps; reps++) {
                // Create a flattened list with the group's children repeated 'reps' times
                List<PatternNode> expandedNodes = new ArrayList<>();
                for (int i = 0; i < reps; i++) {
                    expandedNodes.addAll(groupChildren);
                }
                
                // Append the remaining nodes
                expandedNodes.addAll(remainingNodes);
                
                // Match this expanded pattern
                matchPatternNodes(expandedNodes, startIdx, partialMatch, allMatches);
            }
        }
        
        /**
         * Matches a segment specification repeated a specified number of times
         */
        private void matchRepeatedSegment(SegmentSpecNode node, 
                                         List<List<Sketch>> possibleMatches, 
                                         int repetitions,
                                         int startIdx, 
                                         List<PatternNode> remainingNodes,
                                         List<List<Sketch>> partialMatch,
                                         List<List<List<Sketch>>> allMatches) {
                                         
            // Base case: if repetitions is 0, move to remaining nodes
            if (repetitions == 0) {
                matchPatternNodes(remainingNodes, startIdx, partialMatch, allMatches);
                return;
            }
            
            // For each possible match of this segment
            for (List<Sketch> match : possibleMatches) {
                if (match.isEmpty()) continue;
                
                // Add this match to our partial solution
                List<List<Sketch>> newPartial = new ArrayList<>(partialMatch);
                newPartial.add(match);
                
                int nextIdx = startIdx + match.size();
                
                // If this was the last repetition, proceed to remaining nodes
                if (repetitions == 1) {
                    matchPatternNodes(remainingNodes, nextIdx, newPartial, allMatches);
                } else {
                    // Otherwise, try to match more repetitions of this segment
                    // Get possible matches starting from the next index
                    List<List<Sketch>> nextPossibleMatches = nextIdx < sketches.size() ? 
                        findPossibleMatches(nextIdx, node.getSpec()) : 
                        new ArrayList<>();
                    
                    matchRepeatedSegment(node, nextPossibleMatches, repetitions - 1, 
                                        nextIdx, remainingNodes, newPartial, allMatches);
                }
            }
        }
    }
    
    /**
     * Finds all possible ways to match a single segment at the given starting position,
     * returning a list of subranges (each subrange is a list of Sketch).
     */
    private List<List<Sketch>> findPossibleMatches(int startIndex, SegmentSpecification segment) {
        List<List<Sketch>> possibleMatches = new ArrayList<>();
        
        TimeFilter timeFilter = segment.getTimeFilter();
        ValueFilter valueFilter = segment.getValueFilter();
        
        int minSketches = Math.max(2, timeFilter.getTimeLow()); // Ensure at least 2 sketches for slope
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
