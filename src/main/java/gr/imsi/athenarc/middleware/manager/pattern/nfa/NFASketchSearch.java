package gr.imsi.athenarc.middleware.manager.pattern.nfa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.manager.pattern.QueryExecutor;
import gr.imsi.athenarc.middleware.manager.pattern.Sketch;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

public class NFASketchSearch {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

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
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            return new ArrayList<>();
        }
    
        // 1) Build an NFA that matches patternNodes in sequence
        NFA nfa = buildNfaFromPattern(patternNodes);
        // printNfaGraphically();
        // 2) Run BFS (or a DP approach) to find ALL successful paths
        List<List<List<Sketch>>> allMatches = new ArrayList<>();
        for (int i = 0; i < sketches.size(); i++) {
            allMatches.addAll(simulateNfaAllMatches(nfa, sketches, i));
        }
        return allMatches;
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
        // Build an NFA for the group's children
        NFA childNfa = buildNfaFromPattern(group.getChildren());
        NFAState childStart = childNfa.getStartState();
        
        List<NFAState> childAccepts = new ArrayList<>();
        for (NFAState s : childNfa.getStates()) {
            if (s.isAccept()) {
                childAccepts.add(s);
                s.setAccept(false); // Clear accept flag for now
            }
        }
        
        RepetitionFactor rep = group.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();
        boolean isUnbounded = (maxReps == Integer.MAX_VALUE);
        
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>(childNfa.getStates());
        
        // Create a new start state for the group fragment
        NFAState groupStart = new NFAState();
        states.add(groupStart);
        
        // If no repetitions required (minReps=0), start is an accept state
        if (minReps == 0) {
            frag.acceptStates.add(groupStart);
        }
        
        // Connect to first repetition
        groupStart.getTransitions().add(new Transition(childStart, epsilonMatcher(), "ε"));
        
        if (isUnbounded) {
            // For Kleene-style repetitions, add loop-back transitions
            for (NFAState acceptState : childAccepts) {
                // Add loop-back to start for repeating
                acceptState.getTransitions().add(new Transition(childStart, epsilonMatcher(), "ε"));
                
                // If we've met minimum repetitions, these are accept states
                if (minReps <= 1) {
                    frag.acceptStates.add(acceptState);
                }
            }
            
            // For minReps > 1, we need to track paths through multiple repetitions
            if (minReps > 1) {
                // Need to track more complex accepting states - would require state tracking
                // This is a simplification that will mark all states as accepting after min repetitions
                for (NFAState acceptState : childAccepts) {
                    frag.acceptStates.add(acceptState);
                }
            }
        } else {
            // For finite repetitions
            if (minReps == maxReps) {
                // For exact repetitions, only the final states are accept states
                if (minReps == 1) {
                    frag.acceptStates.addAll(childAccepts);
                } else {
                    // For exact repetitions > 1, would need path tracking 
                    // This is a simplification
                    for (NFAState acceptState : childAccepts) {
                        frag.acceptStates.add(acceptState);
                    }
                }
            } else {
                // For range repetitions (min..max)
                // Mark states as accepting after min repetitions
                if (minReps <= 1) {
                    frag.acceptStates.addAll(childAccepts);
                } else {
                    // For min > 1, would need path tracking
                    // This is a simplification
                    for (NFAState acceptState : childAccepts) {
                        frag.acceptStates.add(acceptState);
                    }
                }
                
                // Add transitions for optional repetitions up to max
                // This is a simplification as tracking exact counts would require additional states
                for (NFAState acceptState : childAccepts) {
                    if (maxReps > 1) {
                        acceptState.getTransitions().add(new Transition(childStart, epsilonMatcher(), "ε"));
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
        for (int count = minSketches; 
             count <= maxSketches && (startIndex + count) <= allSketches.size();
             count++) {
    
            Sketch composite = new Sketch(allSketches.get(startIndex).getFrom(),
                                          allSketches.get(startIndex).getTo());
            List<Sketch> segmentSketches = new ArrayList<>();
            segmentSketches.add(allSketches.get(startIndex));
            composite.addAggregatedDataPoint(allSketches.get(startIndex));
    
            for (int i = 1; i < count; i++) {
                Sketch next = allSketches.get(startIndex + i);
                try {
                    composite.combine(next);
                    segmentSketches.add(next);
                } catch (Exception e) {
                    LOG.error("Failed to combine sketch at index {}: {}", startIndex + i, e.getMessage());
                    break;
                }
            }
    
            if (matchesComposite(composite, valueFilter)) {
                results.add(new MatchResult(count, segmentSketches));
            }
        }
    
        return results;
    }
    
    private boolean matchesComposite(Sketch sketch, ValueFilter valueFilter) {
        if (valueFilter.isValueAny()) {
            return true;
        }
    
        double slope = sketch.computeSlope();
        LOG.debug("Composite slope computed: {} (expected between {} and {})", 
            slope, valueFilter.getValueLow(), valueFilter.getValueHigh());
    
        return (slope >= valueFilter.getValueLow() && slope <= valueFilter.getValueHigh());
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
}