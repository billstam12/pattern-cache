package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.patterncache.PatternCache;
import gr.imsi.athenarc.visual.middleware.patterncache.Sketch;
import gr.imsi.athenarc.visual.middleware.patterncache.query.GroupNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.RepetitionFactor;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SingleNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;

public class NFASketchSearch {
    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);

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
    // ---------------------------------------------
    
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
     * Build an NFA fragment that matches a SingleNode’s segment,
     * repeated min..max times. Here we limit the unrolling based on the number
     * of sketches available.
     */
    private NFAFragment buildSubNfaForSingle(SingleNode single) {
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();
    
        // Create one state for the "start" of the fragment
        NFAState start = new NFAState();
        states.add(start);
    
        RepetitionFactor rep = single.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();
    
        // Determine a lower bound on the number of sketches needed per occurrence.
        int minSketchesForOccurrence = Math.max(2, single.getSpec().getTimeFilter().getTimeLow());
        // Compute an effective maximum repetition to avoid unrolling beyond the available sketches.
        int possibleReps = sketches.size() / minSketchesForOccurrence;
        int effectiveMaxReps = Math.min(maxReps, possibleReps);
    
        List<NFAState> currentTails = new ArrayList<>();
        currentTails.add(start);
    
        for (int i = 1; i <= effectiveMaxReps; i++) {
            // Build a sub-fragment that matches exactly one occurrence.
            NFAFragment onceFrag = buildSingleOccurrenceFragment(single);
            // Link from each state in currentTails to onceFrag.start with an epsilon transition.
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(onceFrag.start, epsilonMatcher(), "ε"));
            }
            states.addAll(onceFrag.allStates);
            // Once we've met the minimum, add the accept states.
            if (i >= minReps) {
                frag.acceptStates.addAll(onceFrag.acceptStates);
            }
            // Prepare for the next iteration.
            currentTails = onceFrag.acceptStates;
        }
    
        // Special case: if minReps==0, the start is also an accept state.
        if (minReps == 0 || frag.acceptStates.isEmpty()) {
            frag.acceptStates.add(start);
        }
    
        frag.start = start;
        frag.allStates = states;
        return frag;
    }
    
    /**
     * Revised method: Build a fragment for a GroupNode.
     * This method gathers the group's children into a sub-NFA and handles repetition
     * in a similar way to SingleNode.
     */
    private NFAFragment buildSubNfaForGroup(GroupNode group) {
        // Build an NFA for the group's children.
        NFA childNfa = buildNfaFromPattern(group.getChildren());
    
        RepetitionFactor rep = group.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();
    
        // Limit the maximum repetition to the number of sketches available.
        int effectiveMaxReps = Math.min(maxReps, sketches.size());
    
        NFAFragment frag = new NFAFragment();
        // Create a new start state for the group fragment.
        NFAState bigStart = new NFAState();
    
        List<NFAState> bigStates = new ArrayList<>();
        bigStates.add(bigStart);
    
        List<NFAState> currentTails = new ArrayList<>();
        currentTails.add(bigStart);
    
        for (int i = 1; i <= effectiveMaxReps; i++) {
            // Link current tails to the child NFA's start state.
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(childNfa.getStartState(), epsilonMatcher(), "ε"));
            }
            bigStates.addAll(childNfa.getStates());
            List<NFAState> childAccepts = new ArrayList<>();
            for (NFAState s : childNfa.getStates()) {
                if (s.isAccept()) {
                    childAccepts.add(s);
                }
            }
            if (i >= minReps) {
                frag.acceptStates.addAll(childAccepts);
            }
            currentTails = childAccepts;
            // Clear the accept flag on child states so they are not reused.
            for (NFAState s : childAccepts) {
                s.setAccept(false);
            }
        }
    
        if (minReps == 0 || frag.acceptStates.isEmpty()) {
            frag.acceptStates.add(bigStart);
        }
    
        frag.start = bigStart;
        frag.allStates = bigStates;
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