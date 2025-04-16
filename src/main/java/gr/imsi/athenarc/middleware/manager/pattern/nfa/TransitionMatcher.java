package gr.imsi.athenarc.middleware.manager.pattern.nfa;

import java.util.List;

import gr.imsi.athenarc.middleware.manager.pattern.Sketch;

@FunctionalInterface
    public interface TransitionMatcher {
        /**
         * Given a start index plus the full list of sketches,
         * return zero or more ways we can match from that position.
         */
        List<MatchResult> matchFrom(int startIndex, List<Sketch> allSketches);
    }