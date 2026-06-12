package gr.imsi.athenarc.middleware.pattern.nfa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.pattern.AdvancementStrategy;
import gr.imsi.athenarc.middleware.pattern.MatchingStrategy;
import gr.imsi.athenarc.middleware.pattern.MatchSelectionStrategy;
import gr.imsi.athenarc.middleware.pattern.PatternQueryManager;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.pattern.PatternMatch;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;

public class NFASketchSearch {
    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final List<Sketch> sketches;
    private final List<PatternNode> patternNodes;
    private final MatchMode matchMode;

    /**
     * Per-segment-spec sliding cache: when the BFS scan calls findPossibleMatches
     * for the same spec at strictly consecutive startIndices, the composite is
     * reused — drop one source sketch's points off the front, append the next
     * sketch's points at the back. Saves the {@code (len-1)} combine calls per
     * scan step (~25% of BFS wall time on long fixed-length segments). Keyed by
     * identity because each pattern node owns a unique SegmentSpecification
     * instance for the lifetime of one NFASketchSearch.
     */
    private final IdentityHashMap<SegmentSpecification, SlideEntry> slideCache = new IdentityHashMap<>();

    private static final class SlideEntry {
        Sketch composite;
        ArrayDeque<Sketch> segmentSketches;
        ArrayDeque<Integer> chunkSizes; // data points contributed by each window sketch, in order
        int lastStartIndex;
        int length;
    }

    public NFASketchSearch(List<Sketch> sketches, List<PatternNode> patternNodes) {
        this(sketches, patternNodes, MatchMode.STRICT);
    }

    public NFASketchSearch(List<Sketch> sketches, List<PatternNode> patternNodes, MatchMode matchMode) {
        this.sketches = sketches;
        this.patternNodes = patternNodes;
        this.matchMode = matchMode == null ? MatchMode.STRICT : matchMode;
    }


    /**
     * Builds the NFA, then runs BFS to collect all matches.
     */
    public List<PatternMatch> findAllMatches() {
        return findMatches(MatchingStrategy.SELECTION, MatchSelectionStrategy.LONGEST, AdvancementStrategy.AFTER_MATCH_END);
    }
    
    /**
     * Builds the NFA, then runs search to collect matches using the specified strategies.
     * @param matchingStrategy Strategy for handling matches
     * @param selectionStrategy Strategy for selecting among multiple matches at the same position
     * @param advancementStrategy Strategy for advancing after finding a match (only used when overlapStrategy is NO_OVERLAPS)
     */
    // ---- coarse hot-path counters, off by default. Flip PROFILE_ENABLED to true
    //      (or set via system property -Dpattern.bfs.profile=true) to dump a
    //      per-query breakdown of where BFS time goes.
    public static boolean PROFILE_ENABLED =
            Boolean.parseBoolean(System.getProperty("pattern.bfs.profile", "false"));
    public static long PROFILE_FIND_POSSIBLE_NS = 0;
    public static long PROFILE_FIND_POSSIBLE_CALLS = 0;
    public static long PROFILE_COMBINE_NS = 0;
    public static long PROFILE_COMBINE_CALLS = 0;
    public static long PROFILE_LP_NS = 0;
    public static long PROFILE_LP_CALLS = 0;
    public static long PROFILE_BOUND_CHECK_NS = 0;
    public static long PROFILE_BOUND_CHECK_CALLS = 0;
    public static long PROFILE_CLONE_NS = 0;
    public static long PROFILE_CLONE_CALLS = 0;

    public List<PatternMatch> findMatches(MatchingStrategy matchingStrategy,
                                               MatchSelectionStrategy selectionStrategy,
                                               AdvancementStrategy advancementStrategy) {
        if (sketches.isEmpty() || patternNodes.isEmpty()) {
            return new ArrayList<>();
        }

        if (PROFILE_ENABLED) {
            PROFILE_FIND_POSSIBLE_NS = 0; PROFILE_FIND_POSSIBLE_CALLS = 0;
            PROFILE_COMBINE_NS = 0; PROFILE_COMBINE_CALLS = 0;
            PROFILE_LP_NS = 0; PROFILE_LP_CALLS = 0;
            PROFILE_BOUND_CHECK_NS = 0; PROFILE_BOUND_CHECK_CALLS = 0;
            PROFILE_CLONE_NS = 0; PROFILE_CLONE_CALLS = 0;
        }
        long bfsStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;

        // 1) Build an NFA that matches patternNodes in sequence
        NFA nfa = buildNfaFromPattern(patternNodes);
        // printNfaGraphically();

        List<PatternMatch> result;
        if (matchingStrategy == MatchingStrategy.ALL) {
            // 2a) Run BFS to find ALL successful paths
            List<PatternMatch> patternMatches = new ArrayList<>();
            for (int i = 0; i < sketches.size(); i++) {
                List<NfaMatch> matchesFromCurrentIndex = simulateNfaAllMatches(nfa, sketches, i);
                for (NfaMatch m : matchesFromCurrentIndex) {
                    patternMatches.add(new PatternMatch(m.segments, m.filters));
                }
            }
            result = patternMatches;
        } else {
            // 2b) Run search to find matches with selection
            List<NfaMatch> matchResults = findMatchesWithSelection(nfa, sketches, selectionStrategy, advancementStrategy);

            List<PatternMatch> patternMatches = new ArrayList<>();
            for (NfaMatch m : matchResults) {
                patternMatches.add(new PatternMatch(m.segments, m.filters));
            }
            result = patternMatches;
        }

        if (PROFILE_ENABLED) {
            long totalNs = System.nanoTime() - bfsStartNs;
            LOG.info("BFS profile (sketches={}, mode={}): total={}ms | findPossibleMatches: {}calls {}ms | combine: {}calls {}ms | LP: {}calls {}ms | boundCheck: {}calls {}ms | clone: {}calls {}ms",
                    sketches.size(), matchMode,
                    totalNs / 1_000_000,
                    PROFILE_FIND_POSSIBLE_CALLS, PROFILE_FIND_POSSIBLE_NS / 1_000_000,
                    PROFILE_COMBINE_CALLS, PROFILE_COMBINE_NS / 1_000_000,
                    PROFILE_LP_CALLS, PROFILE_LP_NS / 1_000_000,
                    PROFILE_BOUND_CHECK_CALLS, PROFILE_BOUND_CHECK_NS / 1_000_000,
                    PROFILE_CLONE_CALLS, PROFILE_CLONE_NS / 1_000_000);
        }
        return result;
    }

    /**
     * NFA-internal match record: parallel lists of segment sketches and the
     * ValueFilter each segment was validated against. Lets the post-NFA classifier
     * in {@code PatternMatch.MatchSegment} answer {@code isConfident()} without an
     * external filters table.
     */
    private static final class NfaMatch {
        final List<List<Sketch>> segments;
        final List<ValueFilter> filters;

        NfaMatch(List<List<Sketch>> segments, List<ValueFilter> filters) {
            this.segments = segments;
            this.filters = filters;
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
    
    /**
     * Persistent cons-cell for the BFS frontier. Each transition produces a new
     * node pointing back at the parent — extending a path is O(1) instead of
     * deep-copying the segment lists. The full {@code List<List<Sketch>>} +
     * {@code List<ValueFilter>} are materialized once per accepted match by
     * walking the parent chain. {@link #ROOT} is the sentinel for "no segments yet".
     */
    private static final class PathNode {
        final PathNode parent;
        final List<Sketch> segment;
        final ValueFilter filter;
        final int depth;

        private PathNode(PathNode parent, List<Sketch> segment, ValueFilter filter) {
            this.parent = parent;
            this.segment = segment;
            this.filter = filter;
            this.depth = parent == null ? 0 : parent.depth + 1;
        }

        static final PathNode ROOT = new PathNode(null, null, null);

        PathNode extend(List<Sketch> seg, ValueFilter f) {
            return new PathNode(this, seg, f);
        }

        NfaMatch materialize() {
            List<List<Sketch>> segments = new ArrayList<>(depth);
            List<ValueFilter> filters = new ArrayList<>(depth);
            // Walk parent chain newest→oldest, then reverse via index assignment
            // so segments end up in match order. Pre-sized arrays avoid Deque
            // allocation on the hot path.
            for (int i = 0; i < depth; i++) {
                segments.add(null);
                filters.add(null);
            }
            int idx = depth - 1;
            for (PathNode p = this; p.parent != null; p = p.parent) {
                segments.set(idx, p.segment);
                filters.set(idx, p.filter);
                idx--;
            }
            return new NfaMatch(segments, filters);
        }
    }

    private List<NfaMatch> simulateNfaAllMatches(NFA nfa, List<Sketch> sketches, int startIndex) {
        List<NfaMatch> allMatches = new ArrayList<>();

        class StateIndexPath {
            final NFAState state;
            final int index;
            final PathNode path;

            StateIndexPath(NFAState s, int i, PathNode p) {
                state = s; index = i; path = p;
            }
        }

        List<StateIndexPath> queue = new ArrayList<>();
        queue.add(new StateIndexPath(nfa.getStartState(), startIndex, PathNode.ROOT));

        for (int idx = 0; idx < queue.size(); idx++) {
            StateIndexPath sip = queue.get(idx);
            NFAState currentState = sip.state;
            int currentIndex = sip.index;
            PathNode currentPath = sip.path;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Visiting state {} at index {} with path depth {}", currentState, currentIndex, currentPath.depth);
            }
            if (currentState.isAccept()) {
                LOG.debug("Reached an accept state at index {}.", currentIndex);
                allMatches.add(currentPath.materialize());
            }
            for (Transition t : currentState.getTransitions()) {
                List<MatchResult> results = t.getMatcher().matchFrom(currentIndex, sketches);
                for (MatchResult matchResult : results) {
                    int nextIndex = currentIndex + matchResult.getConsumedCount();
                    if (nextIndex <= sketches.size()) {
                        // Epsilon transitions (empty consumed) reuse the parent
                        // node — no allocation, no extra depth on the chain.
                        PathNode newPath = matchResult.getMatchedSketches().isEmpty()
                                ? currentPath
                                : currentPath.extend(matchResult.getMatchedSketches(), matchResult.getValueFilter());
                        queue.add(new StateIndexPath(t.getTarget(), nextIndex, newPath));
                    }
                }
            }
        }
        return allMatches;
    }
    
    // ---------------------------------------------
    // Step C: Single-segment matching logic
    // ---------------------------------------------
    private List<MatchResult> findPossibleMatches(SegmentSpecification spec, int startIndex, List<Sketch> allSketches) {
        long fpStart = PROFILE_ENABLED ? System.nanoTime() : 0L;
        if (PROFILE_ENABLED) PROFILE_FIND_POSSIBLE_CALLS++;
        List<MatchResult> results = new ArrayList<>();

        TimeFilter timeFilter = spec.getTimeFilter();
        ValueFilter valueFilter = spec.getValueFilter();

        int minSketches = timeFilter.getTimeLow();
        int maxSketches = timeFilter.getTimeHigh();
        LOG.debug("Trying to match segment at index {}: minSketches={}, maxSketches={}", startIndex, minSketches, maxSketches);

        // First, check if the starting sketch has data
        if (startIndex < allSketches.size() && allSketches.get(startIndex).isEmpty()) {
            LOG.debug("Skipping match at index {} because sketch has no data", startIndex);
            slideCache.remove(spec);
            if (PROFILE_ENABLED) PROFILE_FIND_POSSIBLE_NS += System.nanoTime() - fpStart;
            return results; // Return empty list, can't match segments starting with empty sketches
        }

        for (int count = minSketches;
             count <= maxSketches && (startIndex + count) <= allSketches.size();
             count++) {

            Sketch composite = null;
            ArrayDeque<Sketch> segmentDeque = null;
            boolean validComposite = true;

            SlideEntry cached = slideCache.get(spec);
            boolean slid = false;
            // Fast path: previous call left a composite for [startIndex-1, startIndex-1+count);
            // slide it forward by one source sketch instead of rebuilding from scratch.
            if (cached != null
                    && cached.composite.supportsSliding()
                    && cached.length == count
                    && cached.lastStartIndex == startIndex - 1) {
                Sketch toAdd = allSketches.get(startIndex + count - 1);
                if (cached.composite.canCombineWith(toAdd)) {
                    int headChunk = cached.chunkSizes.pollFirst();
                    cached.segmentSketches.pollFirst();
                    Sketch newFront = allSketches.get(startIndex);
                    cached.composite.removeFrontDataPoints(headChunk, newFront.getFrom());
                    int beforeSize = cached.composite.dataPointCount();
                    long combineStart = PROFILE_ENABLED ? System.nanoTime() : 0L;
                    cached.composite.combine(toAdd);
                    if (PROFILE_ENABLED) {
                        PROFILE_COMBINE_NS += System.nanoTime() - combineStart;
                        PROFILE_COMBINE_CALLS++;
                    }
                    int afterSize = cached.composite.dataPointCount();
                    cached.chunkSizes.addLast(afterSize - beforeSize);
                    cached.segmentSketches.addLast(toAdd);
                    cached.lastStartIndex = startIndex;
                    composite = cached.composite;
                    segmentDeque = cached.segmentSketches;
                    slid = true;
                } else {
                    // Data gap on the leading edge — invalidate, fall through to rebuild.
                    slideCache.remove(spec);
                }
            }

            if (!slid) {
                long cloneStart = PROFILE_ENABLED ? System.nanoTime() : 0L;
                composite = allSketches.get(startIndex).clone();
                if (PROFILE_ENABLED) {
                    PROFILE_CLONE_NS += System.nanoTime() - cloneStart;
                    PROFILE_CLONE_CALLS++;
                }
                boolean trackChunks = composite.supportsSliding();
                segmentDeque = new ArrayDeque<>(count);
                segmentDeque.addLast(allSketches.get(startIndex));
                ArrayDeque<Integer> chunks = trackChunks ? new ArrayDeque<>(count) : null;
                if (trackChunks) {
                    chunks.addLast(composite.dataPointCount());
                }

                // Pre-size the composite to the post-combine total so the inner
                // combine() loop skips its geometric-doubling resize cascade.
                // Pure perf — same final state, same bound.
                if (trackChunks && count > 1) {
                    int expectedTotal = composite.dataPointCount();
                    for (int i = 1; i < count; i++) {
                        Sketch s = allSketches.get(startIndex + i);
                        if (s.supportsSliding()) expectedTotal += s.dataPointCount();
                    }
                    composite.reserveCapacity(expectedTotal);
                }

                for (int i = 1; i < count; i++) {
                    Sketch nextSketch = allSketches.get(startIndex + i);
                    if (!composite.canCombineWith(nextSketch)) {
                        validComposite = false;
                        break;
                    }
                    try {
                        int before = trackChunks ? composite.dataPointCount() : 0;
                        long combineStart = PROFILE_ENABLED ? System.nanoTime() : 0L;
                        composite.combine(nextSketch);
                        if (PROFILE_ENABLED) {
                            PROFILE_COMBINE_NS += System.nanoTime() - combineStart;
                            PROFILE_COMBINE_CALLS++;
                        }
                        if (trackChunks) {
                            chunks.addLast(composite.dataPointCount() - before);
                        }
                        segmentDeque.addLast(nextSketch);
                    } catch (Exception e) {
                        LOG.error("Failed to combine sketch at index {}: {}", startIndex + i, e.getMessage());
                        validComposite = false;
                        break;
                    }
                }

                // Cache only on a clean rebuild of a slide-capable composite —
                // the next consecutive call slides it instead of rebuilding.
                if (validComposite && trackChunks) {
                    SlideEntry e = (cached != null) ? cached : new SlideEntry();
                    e.composite = composite;
                    e.segmentSketches = segmentDeque;
                    e.chunkSizes = chunks;
                    e.lastStartIndex = startIndex;
                    e.length = count;
                    slideCache.put(spec, e);
                } else {
                    slideCache.remove(spec);
                }
            }

            // STRICT: midpoint angle must be in filter (zero-uncertainty matchers — OLS / MATCH_RECOGNIZE).
            // RELAXED: combined-sketch bound must intersect filter — produces a superset of strict matches
            // for the cached approxOLS path, where the post-NFA classifier resolves confident vs. ambiguous.
            long boundStart = PROFILE_ENABLED ? System.nanoTime() : 0L;
            boolean filterPasses = matchMode == MatchMode.RELAXED
                    ? composite.boundIntersects(valueFilter)
                    : composite.matches(valueFilter);
            if (PROFILE_ENABLED) {
                PROFILE_BOUND_CHECK_NS += System.nanoTime() - boundStart;
                PROFILE_BOUND_CHECK_CALLS++;
            }
            if (validComposite && filterPasses) {
                // Snapshot the segment list — the cache may mutate segmentDeque on the next slide.
                List<Sketch> segmentSketches = new ArrayList<>(segmentDeque);
                results.add(new MatchResult(count, segmentSketches, valueFilter));
            }
        }

        if (PROFILE_ENABLED) PROFILE_FIND_POSSIBLE_NS += System.nanoTime() - fpStart;
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
     * Finds matches using the specified selection strategy.
     * When multiple matches start at the same position, selects based on the strategy.
     */
    private List<NfaMatch> findMatchesWithSelection(NFA nfa, List<Sketch> sketches,
                                                              MatchSelectionStrategy selectionStrategy,
                                                              AdvancementStrategy advancementStrategy) {
        List<NfaMatch> matches = new ArrayList<>();
        int currentIndex = 0;

        while (currentIndex < sketches.size()) {
            List<NfaMatch> matchesFromCurrentIndex = simulateNfaAllMatches(nfa, sketches, currentIndex);

            if (!matchesFromCurrentIndex.isEmpty()) {
                NfaMatch selectedMatch = selectMatch(matchesFromCurrentIndex, selectionStrategy);
                matches.add(selectedMatch);

                int advancement = calculateAdvancement(selectedMatch.segments, advancementStrategy);
                currentIndex += advancement;

                LOG.debug("Found match of length {} at index {} using {} selection, advanced by {} using {} strategy, next search starts at {}",
                         calculateMatchLength(selectedMatch.segments), currentIndex - advancement,
                         selectionStrategy, advancement, advancementStrategy, currentIndex);
            } else {
                currentIndex++;
            }
        }

        LOG.info("Found {} selection matches using {} selection and {} advancement strategies",
                matches.size(), selectionStrategy, advancementStrategy);
        return matches;
    }

    /**
     * Selects a match from a list of possible matches based on the specified strategy.
     */
    private NfaMatch selectMatch(List<NfaMatch> matches, MatchSelectionStrategy strategy) {
        if (matches.isEmpty()) {
            throw new IllegalStateException("selectMatch called with empty match list");
        }

        NfaMatch selectedMatch = matches.get(0);
        int selectedLength = calculateMatchLength(selectedMatch.segments);

        switch (strategy) {
            case SHORTEST:
                for (NfaMatch m : matches) {
                    int len = calculateMatchLength(m.segments);
                    if (len < selectedLength) {
                        selectedMatch = m;
                        selectedLength = len;
                    }
                }
                LOG.debug("Selected shortest match with length {} out of {} possible matches",
                         selectedLength, matches.size());
                break;

            case LONGEST:
                for (NfaMatch m : matches) {
                    int len = calculateMatchLength(m.segments);
                    if (len > selectedLength) {
                        selectedMatch = m;
                        selectedLength = len;
                    }
                }
                LOG.debug("Selected longest match with length {} out of {} possible matches",
                         selectedLength, matches.size());
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
                return calculateMatchLength(match);
            case AFTER_MATCH_START:
                return 1;
            default:
                throw new IllegalArgumentException("Unknown advancement strategy: " + strategy);
        }
    }
}