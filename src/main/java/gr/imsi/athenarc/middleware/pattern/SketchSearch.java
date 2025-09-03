package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Demonstrates a recursive approach to match all segments of a PatternQuery
 * against a list of Sketches with configurable overlap and match selection strategies.
 */
public class SketchSearch {
    
    private static final Logger LOG = LoggerFactory.getLogger(SketchSearch.class);
    
    private final List<Sketch> sketches;
    private final List<PatternNode> patternNodes;
    
    public SketchSearch(List<Sketch> sketches, List<PatternNode> patternNodes) {
        this.sketches = sketches;
        this.patternNodes = patternNodes;
    }
    
    /**
     * Runs the search, collects *all* matches (not just the first).
     * Returns the list of matches, where each match is a list-of-lists-of-Sketch.
     */
    public List<List<List<Sketch>>> findAllMatches() {
        return findMatches(MatchingStrategy.ALL, MatchSelectionStrategy.LONGEST, AdvancementStrategy.AFTER_MATCH_END);
    }
    

    /**
     * Runs the search to find matches using the specified strategies.
     * @param overlapStrategy Strategy for handling overlapping matches
     * @param selectionStrategy Strategy for selecting among multiple matches at the same position
     * @param advancementStrategy Strategy for advancing after finding a match (only used when overlapStrategy is NO_OVERLAPS)
     * @return the list of matches, where each match is a list-of-lists-of-Sketch.
     */
    public List<List<List<Sketch>>> findMatches(MatchingStrategy matchingStrategy, 
                                                MatchSelectionStrategy selectionStrategy,
                                                AdvancementStrategy advancementStrategy) {
        
        // Basic checks
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            LOG.warn("Either the sketches list is empty or the pattern has no nodes.");
            return new ArrayList<>(); // empty
        }
        
        if (matchingStrategy == MatchingStrategy.ALL) {
            // Find all possible matches including overlapping ones
            return findAllOverlappingMatches();
        } else {
            // Find non-overlapping matches using the specified selection and advancement strategies
            return findNonOverlappingMatches(selectionStrategy, advancementStrategy);
        }
    }
    
    /**
     * Finds all overlapping matches (original behavior).
     */
    private List<List<List<Sketch>>> findAllOverlappingMatches() {
        List<List<List<Sketch>>> allMatches = new ArrayList<>();
        
        // Start matching from any sketch position
        for (int startIdx = 0; startIdx < sketches.size(); startIdx++) {
            PatternMatcher matcher = new PatternMatcher(sketches);
            List<List<Sketch>> partialMatch = new ArrayList<>();
            matcher.matchPatternNodes(patternNodes, startIdx, partialMatch, allMatches);
        }
        
        return allMatches;
    }
    
    /**
     * Finds non-overlapping matches using the specified selection and advancement strategies.
     * When multiple matches start at the same position, selects based on the selection strategy.
     * After finding a match, advances based on the advancement strategy.
     */
    private List<List<List<Sketch>>> findNonOverlappingMatches(MatchSelectionStrategy selectionStrategy,
                                                              AdvancementStrategy advancementStrategy) {
        List<List<List<Sketch>>> nonOverlappingMatches = new ArrayList<>();
        int currentIndex = 0;
        
        while (currentIndex < sketches.size()) {
            // Find all matches starting from currentIndex
            List<List<List<Sketch>>> matchesFromCurrentIndex = new ArrayList<>();
            PatternMatcher matcher = new PatternMatcher(sketches);
            List<List<Sketch>> partialMatch = new ArrayList<>();
            matcher.matchPatternNodes(patternNodes, currentIndex, partialMatch, matchesFromCurrentIndex);
            
            if (!matchesFromCurrentIndex.isEmpty()) {
                // Select match based on strategy
                List<List<Sketch>> selectedMatch = selectMatch(matchesFromCurrentIndex, selectionStrategy);
                nonOverlappingMatches.add(selectedMatch);
                
                // Advance based on the advancement strategy
                int advancement = calculateAdvancement(selectedMatch, advancementStrategy);
                currentIndex += advancement;
                
                LOG.debug("Found match of length {} at index {} using {} selection, advanced by {} using {} strategy, next search starts at {}", 
                         calculateMatchLength(selectedMatch), currentIndex - advancement, 
                         selectionStrategy, advancement, advancementStrategy, currentIndex);
            } else {
                // No match found at currentIndex, move to next position
                currentIndex++;
            }
        }
        
        LOG.info("Found {} non-overlapping matches using {} selection and {} advancement strategies", 
                nonOverlappingMatches.size(), selectionStrategy, advancementStrategy);
        return nonOverlappingMatches;
    }
    
    /**
     * Selects a match from a list of possible matches based on the specified strategy.
     */
    private List<List<Sketch>> selectMatch(List<List<List<Sketch>>> matches, 
                                          MatchSelectionStrategy strategy) {
        if (matches.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<List<Sketch>> selectedMatch;
        
        switch (strategy) {
                
            case SHORTEST:
                selectedMatch = matches.get(0);
                int shortestLength = calculateMatchLength(selectedMatch);
                
                for (List<List<Sketch>> match : matches) {
                    int currentLength = calculateMatchLength(match);
                    if (currentLength < shortestLength) {
                        selectedMatch = match;
                        shortestLength = currentLength;
                    }
                }
                LOG.debug("Selected shortest match with length {} out of {} possible matches", 
                         shortestLength, matches.size());
                break;
                
            case LONGEST:
                selectedMatch = matches.get(0);
                int longestLength = calculateMatchLength(selectedMatch);
                
                for (List<List<Sketch>> match : matches) {
                    int currentLength = calculateMatchLength(match);
                    if (currentLength > longestLength) {
                        selectedMatch = match;
                        longestLength = currentLength;
                    }
                }
                LOG.debug("Selected longest match with length {} out of {} possible matches", 
                         longestLength, matches.size());
                break;
                
            default:
                throw new IllegalArgumentException("Unknown selection strategy: " + strategy);
        }
        
        return selectedMatch;
    }
    
    /**
     * Calculates the total length (number of sketches) consumed by a match.
     */
    private int calculateMatchLength(List<List<Sketch>> match) {
        int totalLength = 0;
        for (List<Sketch> segment : match) {
            totalLength += segment.size();
        }
        return totalLength;
    }
    
    /**
     * Calculates how much to advance the search position based on the advancement strategy.
     */
    private int calculateAdvancement(List<List<Sketch>> match, AdvancementStrategy strategy) {
        switch (strategy) {
            case AFTER_MATCH_END:
                // Advance by the full match length to avoid any overlap
                return calculateMatchLength(match);
            case AFTER_MATCH_START:
                // Advance by 1 position only, allowing partial overlaps
                return 1;
            default:
                throw new IllegalArgumentException("Unknown advancement strategy: " + strategy);
        }
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
            if (currentNode instanceof SingleNode) {
                matchSingleNode((SingleNode) currentNode, startIdx, remainingNodes, partialMatch, allMatches);
            } else if (currentNode instanceof GroupNode) {
                matchGroupNode((GroupNode) currentNode, startIdx, remainingNodes, partialMatch, allMatches);
            }
        }
        
        /**
         * Matches a SegmentSpecNode against sketches
         */
        private void matchSingleNode(SingleNode node, int startIdx, 
                                         List<PatternNode> remainingNodes, 
                                         List<List<Sketch>> partialMatch, 
                                         List<List<List<Sketch>>> allMatches) {
            
            RepetitionFactor repetition = node.getRepetitionFactor();
            int minReps = repetition.getMinRepetitions();
            int maxReps = repetition.getMaxRepetitions();
            
            // For Kleene star (one or more), we need to try multiple repetitions progressively
            // We'll use a recursive approach to handle this
            matchSegmentWithRepetitions(node, startIdx, remainingNodes, partialMatch, allMatches, 0, minReps, maxReps);
        }
        
        /**
         * Recursively matches a segment with multiple repetitions
         */
        private void matchSegmentWithRepetitions(SingleNode node, int currentIdx,
                                                List<PatternNode> remainingNodes,
                                                List<List<Sketch>> currentMatch, 
                                                List<List<List<Sketch>>> allMatches,
                                                int currentReps, int minReps, int maxReps) {
            
            // If we reached minimum repetitions, we can try to match remaining nodes
            if (currentReps >= minReps) {
                // Clone the current match to avoid modifying it in future recursions
                List<List<Sketch>> matchToUse = new ArrayList<>(currentMatch);
                matchPatternNodes(remainingNodes, currentIdx, matchToUse, allMatches);
            }
            
            // If we're still below max repetitions, try one more repetition
            if (currentReps < maxReps && currentIdx < sketches.size()) {
                // Find all possible ways to match this segment at current position
                List<List<Sketch>> possibleMatches = findPossibleMatches(currentIdx, node.getSpec());
                
                for (List<Sketch> match : possibleMatches) {
                    if (!match.isEmpty()) {
                        // Add this match and continue with one more repetition
                        List<List<Sketch>> updatedMatch = new ArrayList<>(currentMatch);
                        updatedMatch.add(match);
                        
                        int nextIdx = currentIdx + match.size();
                        matchSegmentWithRepetitions(node, nextIdx, remainingNodes, 
                                                   updatedMatch, allMatches, 
                                                   currentReps + 1, minReps, maxReps);
                    }
                }
            }
        }
        
        /**
         * Matches a GroupNode against sketches
         */
        private void matchGroupNode(GroupNode node, int startIdx, 
                                   List<PatternNode> remainingNodes, 
                                   List<List<Sketch>> partialMatch, 
                                   List<List<List<Sketch>>> allMatches) {
            
            RepetitionFactor repetition = node.getRepetitionFactor();
            int minReps = repetition.getMinRepetitions();
            int maxReps = repetition.getMaxRepetitions();
            
            // For Kleene star groups, we need a similar recursive approach
            matchGroupWithRepetitions(node, startIdx, remainingNodes, partialMatch, 
                                    allMatches, 0, minReps, maxReps);
        }
        
        /**
         * Recursively matches a group with multiple repetitions
         */
        private void matchGroupWithRepetitions(GroupNode node, int currentIdx,
                                             List<PatternNode> remainingNodes,
                                             List<List<Sketch>> currentMatch,
                                             List<List<List<Sketch>>> allMatches,
                                             int currentReps, int minReps, int maxReps) {
            
            // If we've reached minimum repetitions, we can try to match remaining nodes
            if (currentReps >= minReps) {
                // Clone the current match to avoid modifying it in future recursions
                List<List<Sketch>> matchToUse = new ArrayList<>(currentMatch);
                matchPatternNodes(remainingNodes, currentIdx, matchToUse, allMatches);
            }
            
            // If we're still below max repetitions, try one more repetition
            if (currentReps < maxReps && currentIdx < sketches.size()) {
                // Create a pattern with just the group's children
                List<PatternNode> groupPattern = new ArrayList<>(node.getChildren());
                
                // Try to match this group once
                PatternMatcher subMatcher = new PatternMatcher(sketches);
                List<List<Sketch>> groupMatches = new ArrayList<>();
                List<List<List<Sketch>>> tempAllMatches = new ArrayList<>();
                subMatcher.matchPatternNodes(groupPattern, currentIdx, groupMatches, tempAllMatches);
                
                // For each way the group matched
                for (List<List<Sketch>> groupMatch : tempAllMatches) {
                    // Calculate the next index after this match
                    int nextIdx = currentIdx;
                    for (List<Sketch> matchPart : groupMatch) {
                        nextIdx += matchPart.size();
                    }
                    
                    // Add this match to our current match
                    List<List<Sketch>> updatedMatch = new ArrayList<>(currentMatch);
                    updatedMatch.addAll(groupMatch);
                    
                    // Continue with one more repetition
                    matchGroupWithRepetitions(node, nextIdx, remainingNodes,
                                           updatedMatch, allMatches,
                                           currentReps + 1, minReps, maxReps);
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
        
        int minSketches = timeFilter.getTimeLow(); // Ensure at least 2 sketches for slope
        int maxSketches = timeFilter.getTimeHigh(); 
        
        // First, check if the starting sketch has data
        if (startIndex < sketches.size() && sketches.get(startIndex).isEmpty()) {
            LOG.debug("Skipping match at index {} because sketch has no data", startIndex);
            return possibleMatches; // Return empty list, can't match segments starting with empty sketches
        }
                
        // We'll iterate from [minSketches..maxSketches], as long as we stay in range
        for (int count = minSketches; 
             count <= maxSketches && (startIndex + count) <= sketches.size(); 
             count++) {
            
            // Build one "composite" from sketches[startIndex .. startIndex+count-1]
            Sketch composite = sketches.get(startIndex).clone();
            List<Sketch> segmentSketches = new ArrayList<>();
            segmentSketches.add(sketches.get(startIndex));
            
            boolean validComposite = true;
            
            for (int i = 1; i < count; i++) {
                Sketch nextSketch = sketches.get(startIndex + i);
                
                // Skip this composite if we encounter a sketch with no data
                if (nextSketch.isEmpty()) {
                    validComposite = false;
                    break;
                }
                
                // Use the canCombineWith check before attempting to combine
                if (!composite.canCombineWith(nextSketch)) {
                    validComposite = false;
                    break;
                }
                
                try {
                    composite.combine(nextSketch);
                    segmentSketches.add(nextSketch);
                } catch (Exception e) {
                    LOG.error("Failed to combine sketches at index {}: {}", startIndex + i, e.getMessage());
                    validComposite = false;
                    break;
                }
            }
            
            // Only consider this match if valid and it meets value constraints
            if (validComposite && composite.matches(valueFilter)) {
                possibleMatches.add(segmentSketches);
            }
        }
        
        return possibleMatches;
    }
    
}
