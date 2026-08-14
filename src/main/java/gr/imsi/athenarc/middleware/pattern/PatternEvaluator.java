package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.pattern.nfa.MatchMode;
import gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Pattern-side error evaluator — runs the NFA over a list of sketches and
 * classifies the resulting matches into confident / ambiguous via each match's
 * mean per-segment bound overshoot. Stateful per {@link #evaluate} call, mirror
 * of visual's {@code VisualEvaluator}.
 *
 * <p>"Confident" vs "ambiguous" comes from the bound classifier on each match,
 * not from the executor's refinement strategy: a match is ambiguous when its
 * bound width straddles the matcher's accept boundary (mean per-segment
 * overshoot exceeds the SLO); otherwise the relaxed-filter verdict is provably
 * inside the strict-filter answer and the match counts as confident.
 */
public class PatternEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternEvaluator.class);

    private Classification last;

    /**
     * Run the relaxed NFA (for APPROX_OLS) or strict NFA (for OLS) and classify
     * the resulting matches against {@code targetSlack = 1 - accuracy}. Stores
     * the result internally; subsequent calls overwrite it.
     */
    public Classification evaluate(List<Sketch> sketches, List<PatternNode> patternNodes,
                                   PatternMethod method, double targetSlack) {
        MatchMode mode = method == PatternMethod.OLS ? MatchMode.STRICT : MatchMode.RELAXED;
        List<PatternMatch> matches = executePatternMatching(sketches, patternNodes, mode);

        List<PatternMatch> confident = new ArrayList<>();
        List<PatternMatch> ambiguous = new ArrayList<>();
        for (PatternMatch m : matches) {
            if (m.isConfident(targetSlack)) confident.add(m);
            else ambiguous.add(m);
        }
        double decisionError = computeDecisionError(confident, ambiguous);
        LOG.info("{} search classified {} matches: confident={}, ambiguous={}, decision-error={}",
                mode, matches.size(), confident.size(), ambiguous.size(), decisionError);

        this.last = new Classification(confident, ambiguous, decisionError);
        return this.last;
    }

    /** Strict NFA only, no classification. Used by the OLS path and the
     *  scoped strict-OLS replay where every accepted match is by construction
     *  confident. */
    public List<PatternMatch> evaluateStrict(List<Sketch> sketches, List<PatternNode> patternNodes) {
        return executePatternMatching(sketches, patternNodes, MatchMode.STRICT);
    }

    public boolean hasError(double targetSlack) {
        return last != null && last.decisionError > targetSlack;
    }

    public double getDecisionError() {
        return last == null ? 0.0 : last.decisionError;
    }

    public List<PatternMatch> getConfident() {
        return last == null ? Collections.emptyList() : last.confident;
    }

    public List<PatternMatch> getAmbiguous() {
        return last == null ? Collections.emptyList() : last.ambiguous;
    }

    /**
     * Time-aligned grouping of regions covered by the currently-ambiguous matches,
     * padded by {@code paddingTimeUnits} on each side, intersected with
     * {@code [from, to]} and merged on the {@code timeUnit} grid. Mirror of
     * visual's {@code getHighErrorIntervals}: the set of regions the refinement
     * ladder targets for re-fetch.
     */
    public List<TimeInterval> getAmbiguousRegions(AggregateInterval timeUnit, int paddingTimeUnits,
                                                  long from, long to) {
        return getAmbiguousRegions(last == null ? Collections.emptyList() : last.ambiguous,
                timeUnit, paddingTimeUnits, from, to);
    }

    /** Static variant for callers holding an ambiguous list from a prior classification. */
    public static List<TimeInterval> getAmbiguousRegions(List<PatternMatch> ambiguous,
                                                        AggregateInterval timeUnit, int paddingTimeUnits,
                                                        long from, long to) {
        if (ambiguous == null || ambiguous.isEmpty()) return Collections.emptyList();
        long unitMs = timeUnit.toDuration().toMillis();
        long paddingMs = (long) paddingTimeUnits * unitMs;
        List<TimeInterval> ranges = new ArrayList<>(ambiguous.size());
        for (PatternMatch m : ambiguous) {
            long paddedFrom = Math.max(from, m.getStartTime() - paddingMs);
            long paddedTo = Math.min(to, m.getEndTime() + paddingMs);
            ranges.add(new TimeRange(paddedFrom, paddedTo));
        }
        return DateTimeUtil.groupIntervals(timeUnit, ranges);
    }

    /**
     * Expected fraction of returned matches that are superset errors (probability-
     * of-existence complement, AQP framing): each ambiguous match contributes its
     * {@link PatternMatch#getMeanBoundOvershoot()}, averaged over every returned
     * match. Confident matches contribute 0 (bound contained in filter). No
     * matches → 0.0.
     */
    public static double computeDecisionError(List<PatternMatch> confident,
                                              List<PatternMatch> ambiguous) {
        int total = confident.size() + ambiguous.size();
        if (total == 0) return 0.0;
        double sum = 0.0;
        for (PatternMatch m : ambiguous) sum += m.getBoundOvershot();
        return sum / total;
    }

    // Matcher selection strategy. Defaults to greedy LONGEST + non-overlap; the
    // §5 recall-attribution experiment flips this to MatchingStrategy.ALL (BFS
    // enumerates every valid match, ignoring selection/advancement) so SkC's
    // candidate set can be compared apples-to-apples against OLS-C's.
    private static volatile MatchingStrategy matchingStrategy = MatchingStrategy.SELECTION;

    public static void setMatchingStrategy(MatchingStrategy strategy) {
        matchingStrategy = strategy;
    }

    private static List<PatternMatch> executePatternMatching(List<Sketch> sketches,
                                                             List<PatternNode> patternNodes,
                                                             MatchMode matchMode) {
        LOG.info("Starting {} search over {} aggregate data with {} strategy, {} selection, and {} advancement strategies.",
                matchMode, sketches.size(),
                matchingStrategy, MatchSelectionStrategy.LONGEST, AdvancementStrategy.AFTER_MATCH_END);

        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes, matchMode);

        long startTime = System.currentTimeMillis();
        List<PatternMatch> matches = sketchSearch.findMatches(
                matchingStrategy, MatchSelectionStrategy.LONGEST, AdvancementStrategy.AFTER_MATCH_END);
        long endTime = System.currentTimeMillis();

        LOG.info("Pattern matching completed in {} ms, found {} matches (mode={})",
                (endTime - startTime), matches.size(), matchMode);
        return matches;
    }

    public static final class Classification {
        public final List<PatternMatch> confident;
        public final List<PatternMatch> ambiguous;
        public final double decisionError;

        public Classification(List<PatternMatch> confident, List<PatternMatch> ambiguous, double decisionError) {
            this.confident = confident;
            this.ambiguous = ambiguous;
            this.decisionError = decisionError;
        }
    }
}
