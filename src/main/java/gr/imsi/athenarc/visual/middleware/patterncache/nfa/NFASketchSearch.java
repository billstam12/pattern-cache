package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

import java.util.ArrayList;
import java.util.List;

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
        LOG.debug("Built NFA with {} states", nfa.getStates().size());
        for(NFAState s : nfa.getStates()) {
            LOG.debug("State: {}", s);
        }
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
        for (PatternNode pn : nodes) {
            // Build sub-NFA
            List<NFAState> newTails = new ArrayList<>();
            NFAFragment fragment = buildSubNfa(pn);

            // For each tail in currentTails, add an epsilon transition to fragment.start
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(fragment.start, epsilonMatcher()));
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
     * handling repetition. We'll do a naive approach for demonstration.
     */
    private NFAFragment buildSubNfa(PatternNode node) {
        if (node instanceof SingleNode) {
            return buildSubNfaForSingle((SingleNode) node);
        } else if (node instanceof GroupNode) {
            return buildSubNfaForGroup((GroupNode) node);
        }
        // If other node types exist, handle them accordingly
        throw new UnsupportedOperationException("Unknown node type: " + node.getClass());
    }

    /**
     * Build an NFA fragment that matches a SingleNode’s segment,
     * repeated min..max times.
     */
    private NFAFragment buildSubNfaForSingle(SingleNode single) {
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();

        // We'll create 1 NFAState for the "start" of the fragment
        NFAState start = new NFAState();
        states.add(start);

        RepetitionFactor rep = single.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();

        // We'll create a chain of sub-fragments for each repetition
        List<NFAState> currentTails = new ArrayList<>();
        currentTails.add(start);

        for (int i = 1; i <= maxReps; i++) {
            // Build a small sub-fragment that matches EXACTLY one SingleNode occurrence
            NFAFragment onceFrag = buildSingleOccurrenceFragment(single);

            // Link from each state in currentTails -> onceFrag.start with epsilon
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(onceFrag.start, epsilonMatcher()));
            }

            // Add onceFrag states to "states"
            states.addAll(onceFrag.allStates);

            // If i >= minReps, the accept states of onceFrag are valid tails
            // because we can stop repeating here if we want
            if (i >= minReps) {
                frag.acceptStates.addAll(onceFrag.acceptStates);
            }

            // unify the accept states from onceFrag so we can chain again
            currentTails = onceFrag.acceptStates;
        }

        // If minReps=0, also consider the start as an accept tail
        if (minReps == 0) {
            frag.acceptStates.add(start);
        }

        // If we never added anything, at least add start
        if (frag.acceptStates.isEmpty()) {
            frag.acceptStates.add(start);
        }

        frag.start = start;
        frag.allStates = states;
        return frag;
    }

    /**
     * Build a fragment that matches exactly 1 occurrence of SingleNode
     * (like your findPossibleMatches logic).
     */
    private NFAFragment buildSingleOccurrenceFragment(SingleNode node) {
        NFAFragment frag = new NFAFragment();
        List<NFAState> states = new ArrayList<>();

        // One start, one accept
        NFAState start = new NFAState();
        NFAState accept = new NFAState();
        states.add(start);
        states.add(accept);

        // Create a transition that attempts to match the segment constraints
        SegmentSpecification spec = node.getSpec();
        TransitionMatcher matcher = (startIndex, allSketches) ->
            findPossibleMatches(spec, startIndex, allSketches);

        start.getTransitions().add(new Transition(accept, matcher));

        frag.start = start;
        frag.acceptStates.add(accept);
        frag.allStates = states;
        return frag;
    }

    /**
     * Build a fragment for a GroupNode: we gather the group's children into a sub-NFA,
     * then handle repetition the same way as for SingleNode. Very simplified here.
     */
    private NFAFragment buildSubNfaForGroup(GroupNode group) {
        // Build an NFA for the group's children
        NFA childNfa = buildNfaFromPattern(group.getChildren());

        // Then handle repetition factor {min, max} in a manner similar to SingleNode
        RepetitionFactor rep = group.getRepetitionFactor();
        int minReps = rep.getMinRepetitions();
        int maxReps = rep.getMaxRepetitions();

        // We'll effectively "wrap" childNfa in a bigger fragment
        NFAFragment frag = new NFAFragment();
        // We'll need one new start
        NFAState bigStart = new NFAState();

        List<NFAState> bigStates = new ArrayList<>();
        bigStates.add(bigStart);

        List<NFAState> currentTails = new ArrayList<>();
        currentTails.add(bigStart);

        for (int i = 1; i <= maxReps; i++) {
            // Link from currentTails -> childNfa.start via epsilon
            for (NFAState tail : currentTails) {
                tail.getTransitions().add(new Transition(childNfa.getStartState(), epsilonMatcher()));
            }

            // Add all states from childNfa
            bigStates.addAll(childNfa.getStates());

            // If i >= minReps, the accept states of childNfa are valid tails
            List<NFAState> childAccepts = new ArrayList<>();
            for (NFAState s : childNfa.getStates()) {
                if (s.isAccept()) {
                    childAccepts.add(s);
                }
            }
            if (i >= minReps) {
                frag.acceptStates.addAll(childAccepts);
            }

            // unify childAccepts so we can chain again
            currentTails = childAccepts;

            // Mark them no longer accept because we’ll keep building
            for (NFAState s : childAccepts) {
                s.setAccept(false);
            }
        }

        // minReps=0 => bigStart is also an accept
        if (minReps == 0) {
            frag.acceptStates.add(bigStart);
        }

        if (frag.acceptStates.isEmpty()) {
            frag.acceptStates.add(bigStart);
        }

        frag.start = bigStart;
        frag.allStates = bigStates;
        return frag;
    }

    private TransitionMatcher epsilonMatcher() {
        return (startIndex, allSketches) -> {
            // An epsilon transition consumes 0 sketches
            List<MatchResult> r = new ArrayList<>();
            r.add(new MatchResult(0, new ArrayList<>()));
            return r;
        };
    }

    // A fragment struct for convenience
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

        // We'll store a queue of (state, index, partialPath),
        // where partialPath is a List<List<Sketch>> describing how we've segmented so far
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
            // If we're in an accept state, that means we've matched the entire pattern so far
            if (currentState.isAccept()) {
                // record the path as a valid match
                allMatches.add(currentPath);
            }

            // Explore transitions
            for (Transition t : currentState.getTransitions()) {
                List<MatchResult> results = t.getMatcher().matchFrom(currentIndex, sketches);
                for (MatchResult matchResult : results) {
                    int nextIndex = currentIndex + matchResult.getConsumedCount();
                    if (nextIndex <= sketches.size()) {
                        // copy the current path
                        List<List<Sketch>> newPath = deepCopy(currentPath);
                        if (!matchResult.getMatchedSketches().isEmpty()) {
                            // if we matched a segment of sketches, add that as a new path segment
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

}
