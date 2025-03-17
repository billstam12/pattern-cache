package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

import java.util.List;

import gr.imsi.athenarc.visual.middleware.patterncache.Sketch;

@FunctionalInterface
    public interface TransitionMatcher {
        /**
         * Given a start index plus the full list of sketches,
         * return zero or more ways we can match from that position.
         */
        List<MatchResult> matchFrom(int startIndex, List<Sketch> allSketches);
    }