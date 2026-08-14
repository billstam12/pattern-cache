package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.pattern.PatternEvaluator.Classification;
import gr.imsi.athenarc.middleware.pattern.PatternUtils.QueryParams;
import gr.imsi.athenarc.middleware.pattern.PatternUtils.RefinementStats;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Pattern executor — refines only the regions covered by still-ambiguous
 * matches, mirroring the visual path's scoped fetch ladder. After an initial
 * relaxed-NFA classify, refinement (and the eventual strict-OLS replay) target
 * just those ambiguous regions; confident regions are never re-touched.
 */
public class PatternQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryExecutor.class);

    /** Pad each scoped ambiguous-region replay (relaxed-refinement at finer α, or
     *  strict-OLS over exact slopes) by this many timeUnits on each side. Gives the
     *  replay NFA endpoint freedom so it can recover matches at slightly-shifted
     *  endpoints — either because strict admission is tighter than the relaxed
     *  bound-intersect, or because finer sub-bucket bounds extend/trim borderline
     *  neighbours. 0 = exact-region replay (no padding). */
    private static final int AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS = 2;

    // Coarse per-phase timers, off by default. Set -Dpattern.phase.profile=true to
    // dump a per-query load/eval/refine/replay breakdown. Mirrors the BFS profiler
    // in NFASketchSearch (pattern.bfs.profile): nanoTime only fires when enabled.
    public static boolean PROFILE_ENABLED =
            Boolean.parseBoolean(System.getProperty("pattern.phase.profile", "false"));

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation) {
        return executePatternQueryWithCache(query, dataSource, cache, method, adaptation, 4, true, 4, false, 20);
    }

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor) {
        return executePatternQueryWithCache(query, dataSource, cache, method, adaptation,
                initialAggregationFactor, true, 4, false, 20);
    }

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment) {
        return executePatternQueryWithCache(query, dataSource, cache, method, adaptation,
                initialAggregationFactor, calendarAlignment, 4, false, 20);
    }

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment,
                                                                  int dataReductionFactor) {
        return executePatternQueryWithCache(query, dataSource, cache, method, adaptation,
                initialAggregationFactor, calendarAlignment, dataReductionFactor, false, 20);
    }

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment,
                                                                  int dataReductionFactor,
                                                                  boolean relaxedCacheReuse) {
        return executePatternQueryWithCache(query, dataSource, cache, method, adaptation,
                initialAggregationFactor, calendarAlignment, dataReductionFactor, relaxedCacheReuse, 20);
    }

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment,
                                                                  int dataReductionFactor,
                                                                  boolean relaxedCacheReuse,
                                                                  int maxRefinementSteps) {
        long startTime = System.currentTimeMillis();

        PatternMethod patternMethod = PatternMethod.from(method);
        if (patternMethod == PatternMethod.SAMPLED_OLS) {
            return SampledPatternQueryExecutor.executePatternQueryWithCache(
                    query, dataSource, cache, method, adaptation, initialAggregationFactor,
                    calendarAlignment, dataReductionFactor, relaxedCacheReuse, maxRefinementSteps);
        }
        QueryParams params = PatternUtils.extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        double targetSlack = 1.0 - params.accuracy;

        PatternDataProcessor processor = new PatternDataProcessor(
                dataSource, patternMethod, adaptation, calendarAlignment);
        PatternEvaluator evaluator = new PatternEvaluator();

        // 4b: refinement is capped so each sub-bucket holds at least
        // {@code dataReductionFactor} raw samples. Past α = timeUnit / (factor *
        // samplingInterval) the sub-bucket grid collapses onto the sampling grid:
        // SQL aggregates degenerate into single-sample rows (min=max=value), the
        // LP bound stops improving, and refinement burns IO for no precision gain.
        int reductionFactor = Math.max(1, dataReductionFactor);
        long sampleIntervalMs = dataSource.getDataset().getSamplingInterval();
        long timeUnitMs = params.timeUnit.toDuration().toMillis();
        int dataResolutionCap = sampleIntervalMs > 0 && timeUnitMs > 0
                ? (int) Math.max(1, Math.min((long) RefinementPredictor.MAX_AGG_FACTOR,
                        timeUnitMs / ((long) reductionFactor * sampleIntervalMs)))
                : RefinementPredictor.MAX_AGG_FACTOR;

        // Each query enters at the configured initial α. We do NOT carry a global
        // per-measure α forward across queries: doing so makes one query's
        // refinement decision invalidate the coarse cache for every subsequent
        // query (their cover-cap rejects the cheap 1h spans), forcing massive
        // fresh fetches at the inherited fine resolution. The cache itself
        // already holds the previously-refined fine slices; when an ambiguous
        // match in a refined region hits this query's refinement pass, the
        // tighter cover-cap admits the cached fine data automatically. So
        // refinement work persists in the cache, not in a sidecar ratchet.
        int currentAggFactor = Math.min(dataResolutionCap, Math.max(1, initialAggregationFactor));

        RefinementStats stats = new RefinementStats();
        stats.initialAggFactor = currentAggFactor;
        stats.finalAggFactor = currentAggFactor;

        PatternUtils.IoStats io = new PatternUtils.IoStats();

        long loadStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;
        List<Sketch> sketches = PatternUtils.generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, patternMethod);
        PatternUtils.populateFromCache(sketches, cache, processor, patternMethod, params,
                currentAggFactor, relaxedCacheReuse, io);
        List<TimeInterval> missingIntervals = PatternDataProcessor.identifyDataMissingIntervals(sketches);
        if (missingIntervals.isEmpty()) {
            LOG.info("All required data available in cache, no need for additional fetching");
        } else {
            List<TimeInterval> groupedMissing = DateTimeUtil.groupIntervals(params.timeUnit, missingIntervals);
            LOG.info("Merged into {} intervals for fetching", groupedMissing.size());
            PatternUtils.fetchAndIngest(sketches, cache, processor, patternMethod, params,
                    currentAggFactor, groupedMissing, io);
        }
        long loadNs = PROFILE_ENABLED ? System.nanoTime() - loadStartNs : 0L;

        // OLS has zero error margin, so every NFA-accepted match is provably confident.
        // No relaxed NFA, no classifier, no refinement — return matches as-is.
        if (patternMethod == PatternMethod.OLS) {
            long evalStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;
            List<PatternMatch> matches = evaluator.evaluateStrict(sketches, patternNodes);
            long evalNs = PROFILE_ENABLED ? System.nanoTime() - evalStartNs : 0L;
            stats.matchesBefore = matches.size();
            stats.matchesAfter = matches.size();
            stats.errorBefore = 0.0;
            stats.errorAfter = 0.0;
            LOG.info("Refinement stats: {}", stats);
            logPhaseProfile(patternMethod, sketches.size(), loadNs, evalNs, 0L, 0L);
            PatternQueryResults olsResults = PatternUtils.createResults(matches, startTime, stats);
            olsResults.setIoCount(io.ioCount());
            olsResults.setCacheHitRatio(io.cacheHitRatio());
            return olsResults;
        }

        // D1/D2 bound-tightness dump at the initial α (no-op when disabled).
        BoundStatsLogger.dumpSketches(currentAggFactor, sketches, params.timeUnit);

        long classifyStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;
        Classification classification = evaluator.evaluate(sketches, patternNodes, patternMethod, targetSlack);
        long evalNs = PROFILE_ENABLED ? System.nanoTime() - classifyStartNs : 0L;
        stats.errorBefore = classification.decisionError;
        stats.matchesBefore = classification.confident.size();

        int chosenAggFactor = currentAggFactor;
        List<PatternMatch> finalConfident = classification.confident;
        List<PatternMatch> finalAmbiguous = classification.ambiguous;
        double finalDecisionError = classification.decisionError;

        // Iterative doubling refinement, with per-region overshoot-trajectory
        // early-stop-and-drop. Walks the α ladder up to {@code maxRefinementSteps}
        // times; each step targets matches still showing useful progress (their
        // region's mean overshoot strictly dropped vs the prior step). A region
        // whose mean overshoot rises under tighter LP constraints is shrinking
        // its slope bound AWAY from the target's [minDeg, maxDeg] — that's a
        // non-match in disguise, and we drop its matches outright rather than
        // pay strict-OLS to confirm what the trajectory already says. Bound
        // width is monotone in α (more sub-buckets ⇒ strictly more LP
        // constraints), so a single rise is signal — not noise — and we don't
        // wait for a second confirmation.
        long refineStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;
        List<PatternMatch> activeForRefinement = new ArrayList<>(finalAmbiguous);
        int stalledDroppedTotal = 0;
        int stepsTaken = 0;
        while (adaptation && finalDecisionError > targetSlack
                && !activeForRefinement.isEmpty()
                && stepsTaken < maxRefinementSteps) {
            OptionalInt next = RefinementPredictor.nextAggFactor(
                    chosenAggFactor, finalDecisionError, targetSlack, dataResolutionCap);
            if (next.isEmpty() || next.getAsInt() == chosenAggFactor) {
                LOG.warn("Refinement capped at aggFactor {} after {} steps (decision-error {} > target {}). Falling through to strict-OLS replay.",
                        chosenAggFactor, stepsTaken, finalDecisionError, targetSlack);
                break;
            }
            int newAggFactor = next.getAsInt();
            LOG.warn("Decision-error {} > target {}. Refinement DOUBLING step {}/{}: aggFactor {} -> {} (confident={}, active={}, droppedSoFar={})",
                    finalDecisionError, targetSlack, stepsTaken + 1, maxRefinementSteps,
                    chosenAggFactor, newAggFactor,
                    finalConfident.size(), activeForRefinement.size(), stalledDroppedTotal);
            chosenAggFactor = newAggFactor;
            stats.refinementTriggered = true;

            // Stalled matches are partitioned out by the helper into stalledBin;
            // we discard them — rising mean overshoot is conclusive non-match
            // evidence, no need to strict-OLS-verify.
            List<PatternMatch> stalledBin = new ArrayList<>();
            Classification refinedScoped = replayRelaxedInAmbiguousRegions(
                    activeForRefinement, patternNodes, cache, processor, evaluator,
                    patternMethod, chosenAggFactor, relaxedCacheReuse,
                    params, targetSlack, stalledBin, io);
            if (!stalledBin.isEmpty()) {
                stalledDroppedTotal += stalledBin.size();
                LOG.info("Dropped {} matches with rising region overshoot (treated as non-matches; total dropped so far={}).",
                        stalledBin.size(), stalledDroppedTotal);
            }

            finalConfident = PatternUtils.combine(finalConfident, refinedScoped.confident);
            activeForRefinement = refinedScoped.ambiguous;
            finalAmbiguous = activeForRefinement;
            finalDecisionError = PatternEvaluator.computeDecisionError(finalConfident, finalAmbiguous);
            stepsTaken++;
        }
        long refineNs = PROFILE_ENABLED ? System.nanoTime() - refineStartNs : 0L;

        // Snapshot the approxOls candidate (confident + ambiguous) before the
        // fallback block can overwrite finalConfident. Lets downstream scoring
        // distinguish approxOls' standalone quality from the end-to-end result.
        List<PatternMatch> approxOlsCandidate = PatternUtils.combine(finalConfident, finalAmbiguous);

        // Refinement at this α did not reach target. Replay the strict NFA against
        // exact OLS data, scoped to the regions covered by ambiguous matches.
        long replayNs = 0L;
        long replayStartNs = PROFILE_ENABLED ? System.nanoTime() : 0L;
        if (finalDecisionError > targetSlack) {
            LOG.warn("Refinement to aggFactor {} did not reach target ({} > {}). Strict-OLS replay over {} ambiguous matches.",
                    chosenAggFactor, finalDecisionError, targetSlack, finalAmbiguous.size());

            List<PatternMatch> replayed = replayStrictOlsInAmbiguousRegions(
                    finalAmbiguous, patternNodes, dataSource, params, io);
            stats.olsVerifiedPromotions = replayed.size();
            finalConfident = PatternUtils.combine(finalConfident, replayed);
            finalAmbiguous = Collections.emptyList();
            finalDecisionError = PatternEvaluator.computeDecisionError(finalConfident, finalAmbiguous);
        }
        replayNs = PROFILE_ENABLED ? System.nanoTime() - replayStartNs : 0L;

        stats.finalAggFactor = chosenAggFactor;

        stats.matchesAfter = finalConfident.size();
        stats.errorAfter = finalDecisionError;
        stats.ambiguousAfter = finalAmbiguous.size();
        if (!finalAmbiguous.isEmpty()) {
            double sumWidth = 0.0;
            double sumOver = 0.0;
            for (PatternMatch m : finalAmbiguous) {
                sumWidth += m.getAverageErrorMargin();
                sumOver += m.getBoundOvershot();
            }
            stats.ambBoundWidthDeg = sumWidth / finalAmbiguous.size();
            stats.ambOvershootFrac = sumOver / finalAmbiguous.size();
        }
        LOG.info("Refinement stats: {}", stats);

        List<PatternMatch> returned = finalAmbiguous.isEmpty()
                ? finalConfident
                : PatternUtils.combine(finalConfident, finalAmbiguous);
        logPhaseProfile(patternMethod, sketches.size(), loadNs, evalNs, refineNs, replayNs);
        PatternQueryResults results = PatternUtils.createResults(returned, approxOlsCandidate, startTime, stats);
        results.setIoCount(io.ioCount());
        results.setCacheHitRatio(io.cacheHitRatio());
        return results;
    }

    /** Dump the per-query phase breakdown when -Dpattern.phase.profile=true.
     *  load = initial sketch gen + cache populate + missing-interval DB fetch/ingest;
     *  eval = NFA evaluation (relaxed classify for approxOls, strict for OLS);
     *  refine = doubling refinement loop; replay = scoped strict-OLS fallback. */
    private static void logPhaseProfile(PatternMethod method, int sketchCount,
                                        long loadNs, long evalNs, long refineNs, long replayNs) {
        if (!PROFILE_ENABLED) return;
        long summedNs = loadNs + evalNs + refineNs + replayNs;
        LOG.info("Phase profile (method={}, sketches={}): load={}ms eval={}ms refine={}ms replay={}ms (summed={}ms)",
                method, sketchCount,
                loadNs / 1_000_000, evalNs / 1_000_000, refineNs / 1_000_000, replayNs / 1_000_000,
                summedNs / 1_000_000);
    }

    /**
     * Scoped relaxed refinement: refetch only the regions covered by previously-
     * ambiguous matches (padded by {@link #AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS})
     * at the finer α, then run a RELAXED NFA pass per region over the refined slice.
     *
     * <p>Region is the implicit tag connecting input matches to output matches
     * across α steps: input matches grouped into region R are replaced by the
     * 0..N output matches the NFA finds in R. Per region we compare mean
     * overshoot before vs after — if the new mean did not strictly drop, the
     * bound is shrinking away from the target. Those region's ambiguous matches
     * are appended to {@code stalledOut} and the caller decides their fate
     * (current policy: drop them as non-matches). The returned {@code
     * Classification.ambiguous} contains only the still-progressing matches.
     */
    private static Classification replayRelaxedInAmbiguousRegions(
            List<PatternMatch> ambiguous, List<PatternNode> patternNodes,
            TimeSeriesCache cache, PatternDataProcessor processor, PatternEvaluator evaluator,
            PatternMethod method, int aggFactor, boolean relaxedCacheReuse,
            QueryParams params, double targetSlack,
            List<PatternMatch> stalledOut, PatternUtils.IoStats io) {
        if (ambiguous == null || ambiguous.isEmpty()) {
            return new Classification(Collections.emptyList(), Collections.emptyList(), 0.0);
        }
        List<TimeInterval> grouped = PatternEvaluator.getAmbiguousRegions(
                ambiguous, params.timeUnit, AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS,
                params.alignedFrom, params.alignedTo);
        LOG.info("Scoped relaxed-refinement over {} ambiguous matches grouped into {} regions (padding={} timeUnits)",
                ambiguous.size(), grouped.size(), AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS);

        List<Sketch> refined = PatternUtils.generateSketches(params.alignedFrom, params.alignedTo,
                params.timeUnit, processor.getDataSource(), method);
        PatternUtils.populateFromCache(refined, cache, processor, method, params,
                aggFactor, relaxedCacheReuse, io);
        PatternUtils.fetchAndIngest(refined, cache, processor, method, params, aggFactor, grouped, io);

        List<PatternMatch> confident = new ArrayList<>();
        List<PatternMatch> stillAmbiguous = new ArrayList<>();
        long unitMs = params.timeUnit.toDuration().toMillis();

        for (TimeInterval region : grouped) {
                int startIdx = (int) ((region.getFrom() - params.alignedFrom) / unitMs);
                int endIdx   = (int) ((region.getTo()   - params.alignedFrom) / unitMs);
                if (startIdx < 0 || endIdx > refined.size() || startIdx >= endIdx) {
                        continue;
                }
                List<Sketch> regionSketches = refined.subList(startIdx, endIdx);
                Classification regionClass = evaluator.evaluate(regionSketches, patternNodes, method, targetSlack);
                confident.addAll(regionClass.confident);
                stillAmbiguous.addAll(regionClass.ambiguous);
        }
        // for (TimeInterval region : grouped) {
        //     int startIdx = (int) ((region.getFrom() - params.alignedFrom) / unitMs);
        //     int endIdx = (int) ((region.getTo() - params.alignedFrom) / unitMs);
        //     if (startIdx < 0 || endIdx > refined.size() || startIdx >= endIdx) {
        //         continue;
        //     }

        //     // Old mean overshoot for input matches whose time range overlaps
        //     // this (merged, padded) region.
        //     double oldSum = 0.0;
        //     int oldCount = 0;
        //     for (PatternMatch in : ambiguous) {
        //         long overlapStart = Math.max(in.getStartTime(), region.getFrom());
        //         long overlapEnd = Math.min(in.getEndTime(), region.getTo());
        //         if (overlapStart < overlapEnd) {
        //             oldSum += in.getBoundOvershot();
        //             oldCount++;
        //         }
        //     }
        //     double oldMean = oldCount > 0 ? oldSum / oldCount : Double.POSITIVE_INFINITY;

        //     List<Sketch> regionSketches = refined.subList(startIdx, endIdx);
        //     Classification regionClass = evaluator.evaluate(regionSketches, patternNodes, method, targetSlack);
        //     confident.addAll(regionClass.confident);

        //     if (regionClass.ambiguous.isEmpty()) continue;
        //     double newSum = 0.0;
        //     for (PatternMatch out : regionClass.ambiguous) {
        //         newSum += out.getBoundOvershot();
        //     }
        //     double newMean = newSum / regionClass.ambiguous.size();
        //     if (newMean >= oldMean) {
        //         stalledOut.addAll(regionClass.ambiguous);
        //     } else {
        //         stillAmbiguous.addAll(regionClass.ambiguous);
        //     }
        // }
        return new Classification(confident, stillAmbiguous,
                PatternEvaluator.computeDecisionError(confident, stillAmbiguous));
    }

    /**
     * Replay the strict NFA against exact OLS data, restricted to the regions covered
     * by still-ambiguous matches.
     */
    private static List<PatternMatch> replayStrictOlsInAmbiguousRegions(
            List<PatternMatch> ambiguous, List<PatternNode> patternNodes,
            DataSource dataSource, QueryParams params, PatternUtils.IoStats io) {
        if (ambiguous == null || ambiguous.isEmpty()) {
            return Collections.emptyList();
        }
        List<TimeInterval> grouped = PatternEvaluator.getAmbiguousRegions(
                ambiguous, params.timeUnit, AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS,
                params.alignedFrom, params.alignedTo);
        LOG.info("Strict-OLS replay over {} ambiguous matches grouped into {} regions (padding={} timeUnits)",
                ambiguous.size(), grouped.size(), AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS);

        PatternDataProcessor olsProcessor = new PatternDataProcessor(
                dataSource, PatternMethod.OLS, /*adaptation*/ false, /*calendarAlignment*/ false);
        PatternEvaluator olsEvaluator = new PatternEvaluator();
        List<Sketch> olsSketches = PatternUtils.generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, PatternMethod.OLS);
        AggregatedDataPoints dataPoints = olsProcessor.getSlopeAggregates(
                params.measure, params.alignedFrom, params.alignedTo, grouped, params.timeUnit);
        olsProcessor.processDatapoints(olsSketches, dataPoints,
                params.alignedFrom, params.alignedTo, params.timeUnit);

        if (io != null) {
            long unitMs = params.timeUnit.toDuration().toMillis();
            long replayBuckets = 0;
            if (unitMs > 0) {
                for (TimeInterval r : grouped) {
                    replayBuckets += Math.max(0L, (r.getTo() - r.getFrom()) / unitMs);
                }
            }
            io.recordUncachedFetch(replayBuckets, PatternUtils.aggSizeFor(PatternMethod.OLS));
        }

        long unitMs = params.timeUnit.toDuration().toMillis();
        List<PatternMatch> replayed = new ArrayList<>();
        for (TimeInterval region : grouped) {
            int startIdx = (int) ((region.getFrom() - params.alignedFrom) / unitMs);
            int endIdx = (int) ((region.getTo() - params.alignedFrom) / unitMs);
            if (startIdx < 0 || endIdx > olsSketches.size() || startIdx >= endIdx) {
                continue;
            }
            List<Sketch> regionSketches = olsSketches.subList(startIdx, endIdx);
            replayed.addAll(olsEvaluator.evaluateStrict(regionSketches, patternNodes));
        }
        LOG.info("Strict-OLS replay produced {} matches across {} regions", replayed.size(), grouped.size());
        return replayed;
    }
}
