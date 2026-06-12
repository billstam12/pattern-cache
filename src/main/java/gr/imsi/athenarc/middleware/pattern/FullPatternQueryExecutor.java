package gr.imsi.athenarc.middleware.pattern;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregationFactorService;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.pattern.PatternEvaluator.Classification;
import gr.imsi.athenarc.middleware.pattern.PatternUtils.QueryParams;
import gr.imsi.athenarc.middleware.pattern.PatternUtils.RefinementStats;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Full-range pattern executor — picks α from a <em>partial</em> classifier
 * over whatever's currently cached, then makes a single batched fetch over the
 * whole query range. Mirror of {@code FullVisualQueryExecutor}.
 *
 * <p>The partial classify is structurally biased toward <em>under</em>-counting
 * ambiguity: the relaxed NFA skips empty sketches inside a candidate segment
 * (see {@code Sketch.combine}), so segments spanning uninitialised regions
 * produce bounds derived from the cached part only. This is the inherent
 * guessiness of full mode — if partial error already exceeds the slack, real
 * error is at least that high, so refining is safe; if partial error is below
 * the slack, real error <em>might</em> still exceed it and we'd be wrong.
 * Accept that trade-off; it's exactly what full-vs-scoped is being A/B'd on.
 *
 * <p>Fetch shape:
 * <ul>
 *   <li><b>partialError &gt; targetSlack</b> → refetch the whole range at
 *       refined α (one trip).</li>
 *   <li><b>partialError ≤ targetSlack</b> → patch only the uninitialised
 *       intervals at current α (one trip).</li>
 * </ul>
 *
 * <p>No iterative refinement, no scoped relaxed/strict-OLS replay.
 */
public class FullPatternQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FullPatternQueryExecutor.class);

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment,
                                                                  int dataReductionFactor,
                                                                  boolean relaxedCacheReuse,
                                                                  int maxRefinementSteps) {
        long startTime = System.currentTimeMillis();

        PatternMethod patternMethod = PatternMethod.from(method);
        QueryParams params = PatternUtils.extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        double targetSlack = 1.0 - params.accuracy;

        PatternDataProcessor processor = new PatternDataProcessor(
                dataSource, patternMethod, adaptation, calendarAlignment);
        PatternEvaluator evaluator = new PatternEvaluator();

        int reductionFactor = Math.max(1, dataReductionFactor);
        long sampleIntervalMs = dataSource.getDataset().getSamplingInterval();
        long timeUnitMs = params.timeUnit.toDuration().toMillis();
        int dataResolutionCap = sampleIntervalMs > 0 && timeUnitMs > 0
                ? (int) Math.max(1, Math.min((long) RefinementPredictor.MAX_AGG_FACTOR,
                        timeUnitMs / ((long) reductionFactor * sampleIntervalMs)))
                : RefinementPredictor.MAX_AGG_FACTOR;

        AggregationFactorService aggFactorService = AggregationFactorService.getInstance();
        int currentAggFactor = Math.min(dataResolutionCap, aggFactorService.getAggFactor(
                params.measure, Math.max(1, initialAggregationFactor)));

        RefinementStats stats = new RefinementStats();
        stats.initialAggFactor = currentAggFactor;
        stats.finalAggFactor = currentAggFactor;

        PatternUtils.IoStats io = new PatternUtils.IoStats();

        // Initial cache populate. Some sketches may remain uninitialised — the
        // partial classify below sees that state directly.
        List<Sketch> sketches = PatternUtils.generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, patternMethod);
        PatternUtils.populateFromCache(sketches, cache, processor, patternMethod, params,
                currentAggFactor, relaxedCacheReuse, io);

        // OLS short-circuit: zero bound width → no refinement, no partial-error
        // bias to worry about. Patch missing at current α, then strict NFA.
        if (patternMethod == PatternMethod.OLS) {
            PatternQueryResults olsResults = PatternUtils.olsShortCircuit(sketches, cache, processor,
                    evaluator, patternNodes, params, currentAggFactor, startTime, stats, io);
            olsResults.setIoCount(io.ioCount());
            olsResults.setCacheHitRatio(io.cacheHitRatio());
            LOG.info("Full-mode (OLS) stats: {}", stats);
            return olsResults;
        }

        // Partial classify — biased under-estimate of decision error (see class javadoc).
        Classification partial = evaluator.evaluate(sketches, patternNodes, patternMethod, targetSlack);
        double partialError = partial.decisionError;
        stats.errorBefore = partialError;
        stats.matchesBefore = partial.confident.size();
        LOG.info("Full-mode partial classify: confident={} ambiguous={} partialError={} targetSlack={}",
                partial.confident.size(), partial.ambiguous.size(), partialError, targetSlack);

        // α decision. Refine only when partial error is large.
        int chosenAggFactor = currentAggFactor;
        boolean refining = false;
        Integer tentativeAggFactor = null;
        if (adaptation && partialError > targetSlack) {
            OptionalInt next = RefinementPredictor.nextAggFactor(
                    currentAggFactor, partialError, targetSlack, dataResolutionCap);
            if (next.isPresent() && next.getAsInt() != currentAggFactor) {
                chosenAggFactor = next.getAsInt();
                refining = true;
                tentativeAggFactor = chosenAggFactor;
                stats.refinementTriggered = true;
                LOG.warn("Full-mode refining: partialError {} > target {}; aggFactor {} → {}",
                        partialError, targetSlack, currentAggFactor, chosenAggFactor);
            } else {
                LOG.warn("Full-mode refinement capped at aggFactor {} (partialError={} > target={}); proceeding with what we have.",
                        currentAggFactor, partialError, targetSlack);
            }
        }

        // One batched fetch. Refining → whole range at chosenAggFactor; else →
        // patch only uninitialised intervals at currentAggFactor.
        if (refining) {
            List<TimeInterval> wholeRange = Collections.singletonList(
                    new TimeRange(params.alignedFrom, params.alignedTo));
            PatternUtils.fetchAndIngest(sketches, cache, processor, patternMethod, params,
                    chosenAggFactor, wholeRange, io);
            // Sketches now hold a mix of current-α and refined-α data. Regenerate
            // and re-populate from cache so each sketch sees a single resolution
            // (the coarsest admissible at refined α — which is the refined-α tile).
            sketches = PatternUtils.generateSketches(
                    params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, patternMethod);
            PatternUtils.populateFromCache(sketches, cache, processor, patternMethod, params,
                    chosenAggFactor, relaxedCacheReuse, io);
        } else {
            List<TimeInterval> missing = PatternDataProcessor.identifyDataMissingIntervals(sketches);
            if (!missing.isEmpty()) {
                List<TimeInterval> grouped = DateTimeUtil.groupIntervals(params.timeUnit, missing);
                PatternUtils.fetchAndIngest(sketches, cache, processor, patternMethod, params,
                        currentAggFactor, grouped, io);
            }
        }

        // Final NFA over fully-populated sketches.
        Classification finalClass = evaluator.evaluate(sketches, patternNodes, patternMethod, targetSlack);

        // Persist α. If refining and we landed under target, commit the refined
        // α. If still over target, advance past it (mirror of scoped's
        // defer-the-write + advance-on-failure logic).
        if (tentativeAggFactor != null) {
            if (finalClass.decisionError <= targetSlack) {
                aggFactorService.setAggFactor(params.measure, tentativeAggFactor);
                stats.finalAggFactor = tentativeAggFactor;
            } else {
                int advanced = RefinementPredictor.nextAggFactor(
                        chosenAggFactor, finalClass.decisionError, targetSlack, dataResolutionCap)
                        .orElse(chosenAggFactor);
                aggFactorService.setAggFactor(params.measure, advanced);
                stats.finalAggFactor = advanced;
            }
        }

        stats.matchesAfter = finalClass.confident.size();
        stats.errorAfter = finalClass.decisionError;
        stats.ambiguousAfter = finalClass.ambiguous.size();
        if (!finalClass.ambiguous.isEmpty()) {
            double sumWidth = 0.0;
            double sumOver = 0.0;
            for (PatternMatch m : finalClass.ambiguous) {
                sumWidth += m.getAverageErrorMargin();
                sumOver += m.getBoundOvershot();
            }
            stats.ambBoundWidthDeg = sumWidth / finalClass.ambiguous.size();
            stats.ambOvershootFrac = sumOver / finalClass.ambiguous.size();
        }
        LOG.info("Full-mode stats: {}", stats);

        List<PatternMatch> returned = finalClass.ambiguous.isEmpty()
                ? finalClass.confident
                : PatternUtils.combine(finalClass.confident, finalClass.ambiguous);
        PatternQueryResults results = PatternUtils.createResults(returned, startTime, stats);
        results.setIoCount(io.ioCount());
        results.setCacheHitRatio(io.cacheHitRatio());
        return results;
    }
}
