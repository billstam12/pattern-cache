package gr.imsi.athenarc.middleware.pattern.nfa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.pattern.PatternQueryManager;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.Sketch;

public class NFASketchSearch {
    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final List<Sketch> sketches;
    private final List<PatternNode> patternNodes;
    
    public NFASketchSearch(List<Sketch> sketches, List<PatternNode> patternNodes) {
        this.sketches = sketches;
        this.patternNodes = patternNodes;
    }
    
    /**
     * Builds the NFA, then runs BFS to collect all matches.
     */
    public List<List<List<Sketch>>> findAllMatches() {
        return findAllMatches(false);
    }
    
    /**
     * Builds the NFA, then runs BFS to collect matches.
     * @param allowOverlapping if true, finds all possible matches including overlapping ones.
     *                        if false, finds non-overlapping matches using a greedy approach.
     */
    public List<List<List<Sketch>>> findAllMatches(boolean allowOverlapping) {
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            return new ArrayList<>();
        }
    
        // 1) Build an NFA that matches patternNodes in sequence
        NFA nfa = buildNfaFromPattern(patternNodes);
        // printNfaGraphically();
        
        if (allowOverlapping) {
            // 2a) Run BFS to find ALL successful paths (original behavior)
            List<List<List<Sketch>>> allMatches = new ArrayList<>();
            for (int i = 0; i < sketches.size(); i++) {
                allMatches.addAll(simulateNfaAllMatches(nfa, sketches, i));
            }
            return allMatches;
        } else {
            // 2b) Run greedy search to find non-overlapping matches
            return findNonOverlappingMatches(nfa, sketches);
        }
    }
    
    // ---------------------------------------------
    // Step A: Build an NFA from the pattern
    // ------------------- --------------------------
    
    private NFA buildNfaFromPattern(List<PatternNode> nodes) {
        NFA nfa = new NFA();
    
        // We'll maintain a "current tail" set of states, initially just [start].
        List<NFAState> currentTails = new ArrayList<>();
        currentTails.add(nfa.getStartState());
    
        // For each PatternNode in top-level order
        for (PatternNode node : nodes) {
            // Build sub-NFA
            List<NFAState> newTails = new ArrayList<>();
            NFAFragment fragment = buildSubNfa(node);
    
            // For each tail in currentTails, add an epsilon transition to fragment.start
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(fragment.start, epsilonMatcher(), "ε"));
            }
            // Merge the fragment states into the NFA's state list
            nfa.getStates().addAll(fragment.allStates);
    
            // Now set currentTails = fragment.acceptStates
            newTails.addAll(fragment.acceptStates);
            currentTails = newTails;
        }
    
        // Mark all current tails as accept states
        for (NFAState s : currentTails) {
            s.setAccept(true);
        }
    
        return nfa;
    }
    
    /**
     * Build a small NFA fragment for a single node (SingleNode or GroupNode),
     * handling repetition.
     */
    private NFAFragment buildSubNfa(PatternNode node) {
        if (node instanceof SingleNode) {
            return buildSubNfaForSingle((SingleNode) node);
        } else if (node instanceof GroupNode) {
            return buildSubNfaForGroup((GroupNode) node);
        }
        throw new UnsupportedOperationException("Unknown node type: " + node.getClass());
    }
    
    /**
     * Build an NFA fragment that matches a SingleNode's segment,
     * handling repetition using loop transitions where appropriate.
     */
    private NFAFragment buildSubNfaForSingle(SingleNode single) {
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();

        // Create start state for the fragment
        NFAState start = new NFAState();
        states.add(start);

        RepetitionFactor rep = single.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();
        boolean isUnbounded = (maxReps == Integer.MAX_VALUE);

        // If no repetitions required (minReps=0), start is also an accept state
        if (minReps == 0) {
            frag.acceptStates.add(start);
        }

        // Build a chain for minimum required repetitions
        NFAState current = start;
        for (int i = 0; i < minReps; i++) {
            NFAFragment onceFrag = buildSingleOccurrenceFragment(single);
            states.addAll(onceFrag.allStates);
            current.getTransitions().add(new Transition(onceFrag.start, epsilonMatcher(), "ε"));
            current = onceFrag.acceptStates.get(0); // Get the only accept state
        }

        // Handle the remaining repetitions based on the type
        if (isUnbounded) {
            // For Kleene-style repetitions (zero-or-more, one-or-more)
            // Create a single instance that loops back
            NFAFragment loopFrag = buildSingleOccurrenceFragment(single);
            states.addAll(loopFrag.allStates);
            
            // Connect current to the loop fragment
            current.getTransitions().add(new Transition(loopFrag.start, epsilonMatcher(), "ε"));
            
            // Add a loop-back transition from the loop's accept state to its start
            NFAState loopAccept = loopFrag.acceptStates.get(0);
            loopAccept.getTransitions().add(new Transition(loopFrag.start, epsilonMatcher(), "ε"));
            
            // Mark current and loop accept state as accept states
            frag.acceptStates.add(current);
            frag.acceptStates.add(loopAccept);
        } else if (maxReps > minReps) {
            // For finite range (min..max), add optional repetitions
            NFAState lastAccept = current;
            frag.acceptStates.add(lastAccept); // minReps is an acceptable state
            
            for (int i = minReps; i < maxReps; i++) {
                NFAFragment onceFrag = buildSingleOccurrenceFragment(single);
                states.addAll(onceFrag.allStates);
                lastAccept.getTransitions().add(new Transition(onceFrag.start, epsilonMatcher(), "ε"));
                lastAccept = onceFrag.acceptStates.get(0);
                frag.acceptStates.add(lastAccept);
            }
        } else if (minReps == maxReps && minReps > 0) {
            // For exact repetitions, the last state is the only accept state
            frag.acceptStates.add(current);
        }

        frag.start = start;
        frag.allStates = states;
        return frag;
    }

    /**
     * Build a fragment for a GroupNode.
     * Uses loop transitions for Kleene-style repetitions.
     */
    private NFAFragment buildSubNfaForGroup(GroupNode group) {
        // Create our result fragment
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();
        
        // Create a new start state for the group fragment
        NFAState groupStart = new NFAState();
        states.add(groupStart);
        
        RepetitionFactor rep = group.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();
        boolean isUnbounded = (maxReps == Integer.MAX_VALUE);
        
        // If no repetitions required (minReps=0), start is an accept state
        if (minReps == 0) {
            frag.acceptStates.add(groupStart);
        }
        
        // Build a chain for minimum required repetitions
        NFAState current = groupStart;
        NFAState lastAccept = null;
        
        // For each required repetition, build a sub-NFA and chain them together
        for (int i = 0; i < minReps; i++) {
            // Build an NFA for the group's children
            NFA childNfa = buildNfaFromPattern(group.getChildren());
            NFAState childStart = childNfa.getStartState();
            
            // Add all child NFA states to our fragment
            states.addAll(childNfa.getStates());
            
            // Connect current state to this repetition
            current.getTransitions().add(new Transition(childStart, epsilonMatcher(), "ε"));
            
            // Find accept states for this repetition
            List<NFAState> childAccepts = new ArrayList<>();
            for (NFAState s : childNfa.getStates()) {
                if (s.isAccept()) {
                    childAccepts.add(s);
                    s.setAccept(false); // Clear accept flag as we'll manage it
                }
            }
            
            // If this is the last required repetition, remember these states
            if (i == minReps - 1) {
                // These states are acceptable after meeting min requirement
                for (NFAState acceptState : childAccepts) {
                    frag.acceptStates.add(acceptState);
                }
                lastAccept = childAccepts.isEmpty() ? null : childAccepts.get(0);
            }
            
            // Update current to the end of this repetition for next iteration
            if (!childAccepts.isEmpty()) {
                // If there are multiple accept states, we need to create a new state to merge them
                if (childAccepts.size() > 1) {
                    NFAState mergeState = new NFAState();
                    states.add(mergeState);
                    for (NFAState acceptState : childAccepts) {
                        acceptState.getTransitions().add(new Transition(mergeState, epsilonMatcher(), "ε"));
                    }
                    current = mergeState;
                } else {
                    current = childAccepts.get(0);
                }
            }
        }
        
        // Now handle optional repetitions (for minReps < maxReps)
        if (isUnbounded && minReps > 0 && lastAccept != null) {
            // For Kleene-style repetitions (one-or-more, etc.)
            // Build one more child NFA for the optional repetition
            NFA optionalNfa = buildNfaFromPattern(group.getChildren());
            NFAState optionalStart = optionalNfa.getStartState();
            
            // Add all states to our fragment
            states.addAll(optionalNfa.getStates());
            
            // Connect from the last accept state to the optional repetition
            lastAccept.getTransitions().add(new Transition(optionalStart, epsilonMatcher(), "ε"));
            
            // Find accept states for the optional repetition
            List<NFAState> optionalAccepts = new ArrayList<>();
            for (NFAState s : optionalNfa.getStates()) {
                if (s.isAccept()) {
                    optionalAccepts.add(s);
                    // These are also accept states
                    frag.acceptStates.add(s);
                }
            }
            
            // Add loop-back transitions from optional accept states to optional start
            for (NFAState acceptState : optionalAccepts) {
                acceptState.getTransitions().add(new Transition(optionalStart, epsilonMatcher(), "ε"));
            }
        } else if (!isUnbounded && maxReps > minReps && lastAccept != null) {
            // For finite range (min..max), add optional repetitions up to max-min times
            for (int i = 0; i < maxReps - minReps; i++) {
                // Build an NFA for one more optional repetition
                NFA optionalNfa = buildNfaFromPattern(group.getChildren());
                NFAState optionalStart = optionalNfa.getStartState();
                
                // Add all states to our fragment
                states.addAll(optionalNfa.getStates());
                
                // Connect from current accept states to this optional repetition
                lastAccept.getTransitions().add(new Transition(optionalStart, epsilonMatcher(), "ε"));
                
                // Find and mark new accept states
                for (NFAState s : optionalNfa.getStates()) {
                    if (s.isAccept()) {
                        frag.acceptStates.add(s);
                        // Update lastAccept for possible next optional repetition
                        lastAccept = s;
                    }
                }
            }
        }
        
        frag.start = groupStart;
        frag.allStates = states;
        return frag;
    }

    /**
     * Build a fragment that matches exactly 1 occurrence of SingleNode.
     */
    private NFAFragment buildSingleOccurrenceFragment(SingleNode node) {
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();
    
        // One start, one accept state.
        NFAState start = new NFAState();
        NFAState accept = new NFAState();
        states.add(start);
        states.add(accept);
    
        // Create a transition that attempts to match the segment constraints.
        SegmentSpecification spec = node.getSpec();
        TransitionMatcher matcher = (startIndex, allSketches) ->
            findPossibleMatches(spec, startIndex, allSketches);
    
        start.getTransitions().add(new Transition(accept, matcher, spec.toString()));
    
        frag.start = start;
        frag.acceptStates.add(accept);
        frag.allStates = states;
        return frag;
    }
    
    private TransitionMatcher epsilonMatcher() {
        return (startIndex, allSketches) -> {
            List<MatchResult> r = new ArrayList<>();
            r.add(new MatchResult(0, new ArrayList<>()));
            return r;
        };
    }
    
    // A fragment struct for convenience.
    private static class NFAFragment {
        NFAState start;
        List<NFAState> acceptStates = new ArrayList<>();
        List<NFAState> allStates = new ArrayList<>();
    }
    
    // ---------------------------------------------
    // Step B: BFS to find all matches
    // ---------------------------------------------
    
    private List<List<List<Sketch>>> simulateNfaAllMatches(NFA nfa, List<Sketch> sketches, int startIndex) {
        List<List<List<Sketch>>> allMatches = new ArrayList<>();
    
        class StateIndexPath {
            NFAState state;
            int index;
            List<List<Sketch>> path;
            
            StateIndexPath(NFAState s, int i, List<List<Sketch>> p) {
                state = s; index = i; path = p;
            }
        }
        
        List<StateIndexPath> queue = new ArrayList<>();
        queue.add(new StateIndexPath(nfa.getStartState(), startIndex, new ArrayList<>()));
    
        for (int idx = 0; idx < queue.size(); idx++) {
            StateIndexPath sip = queue.get(idx);
            NFAState currentState = sip.state;
            int currentIndex = sip.index;
            List<List<Sketch>> currentPath = sip.path;
            LOG.debug("Visiting state {} at index {} with path size {}", currentState, currentIndex, currentPath.size());
            if (currentState.isAccept()) {
                LOG.debug("Reached an accept state at index {}.", currentIndex);
            }
            if (currentState.isAccept()) {
                allMatches.add(currentPath);
            }
            for (Transition t : currentState.getTransitions()) {
                List<MatchResult> results = t.getMatcher().matchFrom(currentIndex, sketches);
                for (MatchResult matchResult : results) {
                    int nextIndex = currentIndex + matchResult.getConsumedCount();
                    if (nextIndex <= sketches.size()) {
                        List<List<Sketch>> newPath = deepCopy(currentPath);
                        if (!matchResult.getMatchedSketches().isEmpty()) {
                            newPath.add(matchResult.getMatchedSketches());
                        }
                        queue.add(new StateIndexPath(t.getTarget(), nextIndex, newPath));
                    }
                }
            }  
        }
        return allMatches;
    }
    
    private List<List<Sketch>> deepCopy(List<List<Sketch>> original) {
        List<List<Sketch>> copy = new ArrayList<>();
        for (List<Sketch> seg : original) {
            copy.add(new ArrayList<>(seg));
        }
        return copy;
    }
    
    // ---------------------------------------------
    // Step C: Single-segment matching logic
    // ---------------------------------------------
    private List<MatchResult> findPossibleMatches(SegmentSpecification spec, int startIndex, List<Sketch> allSketches) {
        List<MatchResult> results = new ArrayList<>();
    
        TimeFilter timeFilter = spec.getTimeFilter();
        ValueFilter valueFilter = spec.getValueFilter();
    
        int minSketches = Math.max(2, timeFilter.getTimeLow());
        int maxSketches = timeFilter.getTimeHigh() + 1;
        LOG.debug("Trying to match segment at index {}: minSketches={}, maxSketches={}", startIndex, minSketches, maxSketches);
        
        // First, check if the starting sketch has data
        if (startIndex < allSketches.size() && allSketches.get(startIndex).isEmpty()) {
            LOG.debug("Skipping match at index {} because sketch has no data", startIndex);
            return results; // Return empty list, can't match segments starting with empty sketches
        }
        
        for (int count = minSketches; 
             count <= maxSketches && (startIndex + count) <= allSketches.size();
             count++) {
    
            Sketch composite = allSketches.get(startIndex).clone();
            List<Sketch> segmentSketches = new ArrayList<>();
            segmentSketches.add(allSketches.get(startIndex));
            boolean validComposite = true;
            
            for (int i = 1; i < count; i++) {
                Sketch nextSketch = allSketches.get(startIndex + i);
                // Skip this composite if we encounter a sketch with no data
                 if (!composite.canCombineWith(nextSketch)) {
                    validComposite = false;
                    break;
                }
                
                try {
                    composite.combine(nextSketch);
                    segmentSketches.add(nextSketch);
                } catch (Exception e) {
                    LOG.error("Failed to combine sketch at index {}: {}", startIndex + i, e.getMessage());
                    validComposite = false; // Consider combination failure as having an "empty" segment
                    break;
                }
            }
    
            // Only consider this match if there were no empty sketches and it meets value constraints
            if (validComposite && composite.matches(valueFilter)) {
                results.add(new MatchResult(count, segmentSketches));
            }
        }
    
        return results;
    }
    
    // ---------------------------
    // Graphical Print Methods
    // ---------------------------
    
    public String toDotFormat() {
        NFA nfa = buildNfaFromPattern(patternNodes);
        StringBuilder sb = new StringBuilder();
        sb.append("digraph NFA {\n");
        sb.append("  rankdir=LR;\n");
        sb.append("  node [shape = circle];\n");
    
        Map<NFAState, Integer> stateIds = new HashMap<>();
        int id = 0;
        for (NFAState state : nfa.getStates()) {
            stateIds.put(state, id);
            if (state.isAccept()) {
                sb.append(String.format("  %d [shape=doublecircle, label=\"%d\"];\n", id, id));
            } else {
                sb.append(String.format("  %d [label=\"%d\"];\n", id, id));
            }
            id++;
        }
    
        for (NFAState state : nfa.getStates()) {
            int fromId = stateIds.get(state);
            for (Transition transition : state.getTransitions()) {
                int toId = stateIds.get(transition.getTarget());
                String label = transition.getLabel();
                sb.append(String.format("  %d -> %d [label=\"%s\"];\n", fromId, toId, label));
            }
        }
        sb.append("}\n");
        return sb.toString();
    }
    
    public void printNfaGraphically() {
        String dotRepresentation = toDotFormat();
        System.out.println(dotRepresentation);
    }
    
    /**
     * Finds non-overlapping matches using a greedy approach.
     * Once a match is found, the algorithm skips ahead to avoid overlapping matches.
     */
    private List<List<List<Sketch>>> findNonOverlappingMatches(NFA nfa, List<Sketch> sketches) {
        List<List<List<Sketch>>> nonOverlappingMatches = new ArrayList<>();
        int currentIndex = 0;
        
        while (currentIndex < sketches.size()) {
            // Try to find a match starting from currentIndex
            List<List<List<Sketch>>> matchesFromCurrentIndex = simulateNfaAllMatches(nfa, sketches, currentIndex);
            
            if (!matchesFromCurrentIndex.isEmpty()) {
                // Found at least one match starting from currentIndex
                // Select the first match (greedy approach) 
                List<List<Sketch>> selectedMatch = matchesFromCurrentIndex.get(0);
                nonOverlappingMatches.add(selectedMatch);
                
                // Calculate the end index of this match to avoid overlaps
                int matchLength = calculateMatchLength(selectedMatch);
                currentIndex += matchLength;
                
                LOG.debug("Found match of length {} at index {}, next search starts at {}", 
                         matchLength, currentIndex - matchLength, currentIndex);
            } else {
                // No match found at currentIndex, move to next position
                currentIndex++;
            }
        }
        
        LOG.info("Found {} non-overlapping matches", nonOverlappingMatches.size());
        return nonOverlappingMatches;
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
}