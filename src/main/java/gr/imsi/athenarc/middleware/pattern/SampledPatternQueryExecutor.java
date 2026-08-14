package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoint;
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
 * Pattern executor over {@link gr.imsi.athenarc.middleware.sketch.SampledOLSSketch}.
 * Draws an initial per-bucket sample over the whole range, classifies with the
 * relaxed NFA, and resolves ambiguous segments by drawing additional samples only
 * in the ambiguous regions until the decision error meets the target or the
 * per-bucket budget reaches the full raw count. Points accumulate in a store, so
 * each refinement fetches only the new rank band.
 */
public class SampledPatternQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SampledPatternQueryExecutor.class);

    private static final int AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS = 2;

    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource,
                                                                  TimeSeriesCache cache, String method, boolean adaptation,
                                                                  int initialAggregationFactor,
                                                                  boolean calendarAlignment,
                                                                  int dataReductionFactor,
                                                                  boolean relaxedCacheReuse,
                                                                  int maxRefinementSteps) {
        long startTime = System.currentTimeMillis();

        PatternMethod patternMethod = PatternMethod.SAMPLED_OLS;
        QueryParams params = PatternUtils.extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        double targetSlack = 1.0 - params.accuracy;

        PatternDataProcessor processor = new PatternDataProcessor(
                dataSource, patternMethod, adaptation, calendarAlignment);
        PatternEvaluator evaluator = new PatternEvaluator();

        int reductionFactor = Math.max(1, dataReductionFactor);
        long sampleIntervalMs = dataSource.getDataset().getSamplingInterval();
        long timeUnitMs = params.timeUnit.toDuration().toMillis();
        int fullSampleCap = sampleIntervalMs > 0 && timeUnitMs > 0
                ? (int) Math.max(1, Math.min((long) RefinementPredictor.MAX_AGG_FACTOR,
                        timeUnitMs / ((long) reductionFactor * sampleIntervalMs)))
                : RefinementPredictor.MAX_AGG_FACTOR;
        int sampleBudget = Math.min(fullSampleCap, Math.max(2, initialAggregationFactor));

        RefinementStats stats = new RefinementStats();
        stats.initialAggFactor = sampleBudget;
        stats.finalAggFactor = sampleBudget;
        PatternUtils.IoStats io = new PatternUtils.IoStats();

        Map<Integer, List<DataPoint>> store = new HashMap<>();
        List<TimeInterval> wholeRange = new ArrayList<>();
        wholeRange.add(new TimeRange(params.alignedFrom, params.alignedTo));

        int used = accumulate(store, processor.getRawSampleDelta(
                params.measure, wholeRange, params.alignedFrom, params.alignedTo, params.timeUnit, 0, sampleBudget));
        io.recordUncachedFetch(used, PatternUtils.aggSizeFor(patternMethod));

        List<Sketch> sketches = buildSketches(store, dataSource, params);
        Classification classification = evaluator.evaluate(sketches, patternNodes, patternMethod, targetSlack);
        stats.errorBefore = classification.decisionError;
        stats.matchesBefore = classification.confident.size();

        List<PatternMatch> finalConfident = classification.confident;
        List<PatternMatch> finalAmbiguous = classification.ambiguous;
        double finalDecisionError = classification.decisionError;

        int stepsTaken = 0;
        while (adaptation && finalDecisionError > targetSlack
                && !finalAmbiguous.isEmpty()
                && stepsTaken < maxRefinementSteps) {
            OptionalInt next = RefinementPredictor.nextAggFactor(
                    sampleBudget, finalDecisionError, targetSlack, fullSampleCap);
            if (next.isEmpty() || next.getAsInt() == sampleBudget) {
                break;
            }
            int newBudget = next.getAsInt();
            stats.refinementTriggered = true;

            List<TimeInterval> regions = PatternEvaluator.getAmbiguousRegions(
                    finalAmbiguous, params.timeUnit, AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS,
                    params.alignedFrom, params.alignedTo);
            int delta = accumulate(store, processor.getRawSampleDelta(
                    params.measure, regions, params.alignedFrom, params.alignedTo, params.timeUnit,
                    sampleBudget, newBudget));
            io.recordUncachedFetch(delta, PatternUtils.aggSizeFor(patternMethod));
            sampleBudget = newBudget;

            List<Sketch> refined = buildSketches(store, dataSource, params);
            Classification c = classifyRegions(refined, regions, evaluator, patternNodes, params, targetSlack);
            finalConfident = PatternUtils.combine(finalConfident, c.confident);
            finalAmbiguous = c.ambiguous;
            finalDecisionError = PatternEvaluator.computeDecisionError(finalConfident, finalAmbiguous);
            stepsTaken++;
        }

        List<PatternMatch> sampledCandidate = PatternUtils.combine(finalConfident, finalAmbiguous);

        if (finalDecisionError > targetSlack && !finalAmbiguous.isEmpty()) {
            stats.fallbackTriggered = true;
            List<PatternMatch> replayed = replayStrictOlsInAmbiguousRegions(
                    finalAmbiguous, patternNodes, dataSource, params, io);
            stats.olsVerifiedPromotions = replayed.size();
            finalConfident = PatternUtils.combine(finalConfident, replayed);
            finalAmbiguous = Collections.emptyList();
            finalDecisionError = PatternEvaluator.computeDecisionError(finalConfident, finalAmbiguous);
        }

        stats.finalAggFactor = sampleBudget;
        stats.matchesAfter = finalConfident.size();
        stats.errorAfter = finalDecisionError;
        stats.ambiguousAfter = finalAmbiguous.size();
        LOG.info("Sampled refinement stats: {}", stats);

        List<PatternMatch> returned = finalAmbiguous.isEmpty()
                ? finalConfident
                : PatternUtils.combine(finalConfident, finalAmbiguous);
        PatternQueryResults results = PatternUtils.createResults(returned, sampledCandidate, startTime, stats);
        results.setIoCount(io.ioCount());
        results.setCacheHitRatio(io.cacheHitRatio());
        return results;
    }

    private static int accumulate(Map<Integer, List<DataPoint>> store, Map<Integer, List<DataPoint>> delta) {
        int added = 0;
        for (Map.Entry<Integer, List<DataPoint>> e : delta.entrySet()) {
            store.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
            added += e.getValue().size();
        }
        return added;
    }

    private static List<Sketch> buildSketches(Map<Integer, List<DataPoint>> store, DataSource dataSource,
                                              QueryParams params) {
        List<Sketch> sketches = PatternUtils.generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, PatternMethod.SAMPLED_OLS);
        for (Map.Entry<Integer, List<DataPoint>> e : store.entrySet()) {
            int idx = e.getKey();
            if (idx < 0 || idx >= sketches.size()) continue;
            Sketch sk = sketches.get(idx);
            for (DataPoint dp : e.getValue()) {
                sk.addDataPoint(dp);
            }
        }
        return sketches;
    }

    private static Classification classifyRegions(List<Sketch> sketches, List<TimeInterval> regions,
            PatternEvaluator evaluator, List<PatternNode> patternNodes, QueryParams params, double targetSlack) {
        List<PatternMatch> confident = new ArrayList<>();
        List<PatternMatch> ambiguous = new ArrayList<>();
        long unitMs = params.timeUnit.toDuration().toMillis();
        for (TimeInterval region : regions) {
            int startIdx = (int) ((region.getFrom() - params.alignedFrom) / unitMs);
            int endIdx   = (int) ((region.getTo()   - params.alignedFrom) / unitMs);
            if (startIdx < 0 || endIdx > sketches.size() || startIdx >= endIdx) {
                continue;
            }
            Classification c = evaluator.evaluate(sketches.subList(startIdx, endIdx),
                    patternNodes, PatternMethod.SAMPLED_OLS, targetSlack);
            confident.addAll(c.confident);
            ambiguous.addAll(c.ambiguous);
        }
        return new Classification(confident, ambiguous,
                PatternEvaluator.computeDecisionError(confident, ambiguous));
    }

    private static List<PatternMatch> replayStrictOlsInAmbiguousRegions(
            List<PatternMatch> ambiguous, List<PatternNode> patternNodes,
            DataSource dataSource, QueryParams params, PatternUtils.IoStats io) {
        if (ambiguous == null || ambiguous.isEmpty()) {
            return Collections.emptyList();
        }
        List<TimeInterval> grouped = PatternEvaluator.getAmbiguousRegions(
                ambiguous, params.timeUnit, AMBIGUOUS_REGION_REPLAY_PADDING_TIME_UNITS,
                params.alignedFrom, params.alignedTo);

        PatternDataProcessor olsProcessor = new PatternDataProcessor(
                dataSource, PatternMethod.OLS, false, false);
        PatternEvaluator olsEvaluator = new PatternEvaluator();
        List<Sketch> olsSketches = PatternUtils.generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, PatternMethod.OLS);
        AggregatedDataPoints dataPoints = olsProcessor.getSlopeAggregates(
                params.measure, params.alignedFrom, params.alignedTo, grouped, params.timeUnit);
        olsProcessor.processDatapoints(olsSketches, dataPoints,
                params.alignedFrom, params.alignedTo, params.timeUnit);

        long unitMs = params.timeUnit.toDuration().toMillis();
        if (io != null && unitMs > 0) {
            long replayBuckets = 0;
            for (TimeInterval r : grouped) {
                replayBuckets += Math.max(0L, (r.getTo() - r.getFrom()) / unitMs);
            }
            io.recordUncachedFetch(replayBuckets, PatternUtils.aggSizeFor(PatternMethod.OLS));
        }

        List<PatternMatch> replayed = new ArrayList<>();
        for (TimeInterval region : grouped) {
            int startIdx = (int) ((region.getFrom() - params.alignedFrom) / unitMs);
            int endIdx = (int) ((region.getTo() - params.alignedFrom) / unitMs);
            if (startIdx < 0 || endIdx > olsSketches.size() || startIdx >= endIdx) {
                continue;
            }
            replayed.addAll(olsEvaluator.evaluateStrict(olsSketches.subList(startIdx, endIdx), patternNodes));
        }
        return replayed;
    }
}
