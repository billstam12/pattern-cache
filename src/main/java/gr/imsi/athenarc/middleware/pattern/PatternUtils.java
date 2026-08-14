package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.CoveredSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.ApproxOLSSketch;
import gr.imsi.athenarc.middleware.sketch.OLSSketch;
import gr.imsi.athenarc.middleware.sketch.SampledOLSSketch;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Pattern utilities — public entry points that don't fit either scope executor,
 * plus package-private building blocks used by {@link
 * PatternQueryExecutor}: pure data
 * extraction, sketch factory, cache+processor composition, result building, and
 * the per-query stats container. Mirror of visual's {@code VisualUtils}.
 */
public final class PatternUtils {

    private PatternUtils() {}

    private static final Logger LOG = LoggerFactory.getLogger(PatternUtils.class);

    /**
     * Execute a pattern query directly without using cache. Strict-NFA only;
     * routes MATCH_RECOGNIZE queries to the dedicated executor.
     */
    public static PatternQueryResults executePatternQuery(PatternQuery query, DataSource dataSource,
                                                          String type, String method) {
        long startTime = System.currentTimeMillis();

        if ("matchRecognize".equals(type)) {
            return MatchRecognizeQueryExecutor.executeMatchRecognizeQuery(query, dataSource, method);
        }

        PatternMethod patternMethod = PatternMethod.from(method);
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();

        PatternDataProcessor processor = new PatternDataProcessor(
                dataSource, patternMethod, /*adaptation*/ false, /*calendarAlignment*/ false);
        PatternEvaluator evaluator = new PatternEvaluator();

        List<Sketch> sketches = generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, dataSource, patternMethod);
        processor.populateFromDataSource(sketches, params.measure,
                params.alignedFrom, params.alignedTo, params.timeUnit, false);

        List<PatternMatch> matches = evaluator.evaluateStrict(sketches, patternNodes);
        PatternQueryResults results = createResults(matches, startTime, null);
        // No-cache OLS pulls 5 regression sums per outer bucket (count, sum,
        // sum_t, sum_t², sum_tv); report bytes (× 8) so the unit matches the
        // Cache Size column and the cached-method IO accountant.
        results.setIoCount(5L * 8L * sketches.size());
        return results;
    }

    // ── Package-private helpers shared by the cached executors ──────────────

    /** Per-query IO accountant for cached pattern queries. populateFromCache /
     *  fetchAndIngest stash time-coverage and span-bytes here; the executor
     *  reads {@link #ioCount()} and {@link #cacheHitRatio()} when building the
     *  result. IO is in <b>bytes</b> — same unit as the {@code Cache Size}
     *  column ({@code CacheMemoryManager.trackSpanAddition} uses the same
     *  {@link TimeSeriesSpan#calculateDeepMemorySize()}), so {@code IO Count ≈
     *  ΔCache Size} per query (modulo eviction). */
    static final class IoStats {
        long hitMs;
        long missMs;
        long ioBytes;

        void recordCacheCovered(long ms) {
            if (ms > 0) hitMs += ms;
        }

        void recordCachedFetch(List<TimeSeriesSpan> spans) {
            for (TimeSeriesSpan s : spans) {
                long span = s.getTo() - s.getFrom();
                if (span > 0) missMs += span;
                ioBytes += s.calculateDeepMemorySize();
            }
        }

        /** Strict-OLS replay (uncached): no cache state change, but values are
         *  pulled from the DB and should count toward total IO. No span is
         *  allocated on this path, so bytes are estimated as {@code buckets ×
         *  aggSize × 8} (data only, excluding the ~80-byte span object overhead
         *  that {@link #recordCachedFetch} includes via deepMemorySize). */
        void recordUncachedFetch(long buckets, int aggSize) {
            if (buckets > 0) ioBytes += buckets * aggSize * 8L;
        }

        long ioCount() { return ioBytes; }

        double cacheHitRatio() {
            long total = hitMs + missMs;
            return total > 0 ? (double) hitMs / total : 0.0;
        }
    }

    /** Used by {@link IoStats#recordUncachedFetch} (strict-OLS replay) where no
     *  span object exists to query {@code calculateDeepMemorySize()} from. */
    static int aggSizeFor(PatternMethod method) {
        switch (method) {
            case OLS:         return 7;
            case APPROX_OLS:  return 4;
            case SAMPLED_OLS: return 2;
            default: throw new IllegalStateException("Unknown method: " + method);
        }
    }

    static QueryParams extractQueryParams(PatternQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasures().get(0); // for now pattern queries have only one measure
        AggregateInterval timeUnit = query.getTimeUnit();
        long alignedFrom = DateTimeUtil.alignToTimeUnitBoundary(from, timeUnit, true);
        long alignedTo = DateTimeUtil.alignToTimeUnitBoundary(to, timeUnit, false);
        double accuracy = query.getAccuracy();
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        return new QueryParams(alignedFrom, alignedTo, accuracy, measure, timeUnit);
    }

    static List<Sketch> generateSketches(long from, long to, AggregateInterval timeUnit,
                                         DataSource dataSource, PatternMethod method) {
        List<Sketch> sketches = new ArrayList<>();
        long unitDurationMs = timeUnit.toDuration().toMillis();
        int numIntervals = DateTimeUtil.numberOfIntervals(from, to, timeUnit);
        long alignedStart = DateTimeUtil.alignToTimeUnitBoundary(
                dataSource.getDataset().getTimeRange().getFrom(), timeUnit, true);

        for (int i = 0; i < numIntervals; i++) {
            long sketchStart = from + (i * unitDurationMs);
            long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
            long bucketId = Math.floorDiv(sketchStart - alignedStart, unitDurationMs);
            Sketch sketch;
            switch (method) {
                case APPROX_OLS:
                    sketch = new ApproxOLSSketch(sketchStart, sketchEnd, bucketId);
                    break;
                case OLS:
                    sketch = new OLSSketch(sketchStart, sketchEnd, bucketId);
                    break;
                case SAMPLED_OLS: {
                    boolean bootstrap = "bootstrap".equalsIgnoreCase(
                            System.getProperty("pattern.sampled.ci", "closed"));
                    if (bootstrap) {
                        sketch = new SampledOLSSketch(
                                sketchStart, sketchEnd, bucketId,
                                (double) unitDurationMs, alignedStart,
                                SampledOLSSketch.DEFAULT_BOOTSTRAP_REPLICATES,
                                SampledOLSSketch.DEFAULT_CI_LEVEL,
                                bucketId);
                    } else {
                        sketch = new SampledOLSSketch(
                                sketchStart, sketchEnd, bucketId,
                                (double) unitDurationMs, alignedStart,
                                SampledOLSSketch.DEFAULT_CRITICAL_VALUE);
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("Unhandled pattern method: " + method);
            }
            sketches.add(sketch);
        }
        return sketches;
    }

    /**
     * Cache populate via {@link TimeSeriesCache#getCoarsestPerRegionCoverage}: a
     * non-overlapping cover where each sub-range owns the coarsest admissible
     * span. Per method the admissibility cap is the resolution that method's
     * sketches would otherwise fetch:
     * <ul>
     *   <li>{@code OLS}        → cap = {@code timeUnit} (one bucket per sketch).</li>
     *   <li>{@code APPROX_OLS} → cap = sub-bucket = {@code timeUnit / α}.</li>
     * </ul>
     */
    static void populateFromCache(List<Sketch> sketches, TimeSeriesCache cache,
                                  PatternDataProcessor processor, PatternMethod method,
                                  QueryParams params, int aggFactor, boolean relaxedCacheReuse,
                                  IoStats io) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        AggregateInterval maxAgg = method == PatternMethod.OLS
                ? params.timeUnit
                : processor.computeSubInterval(params.timeUnit, aggFactor);
        AggregateInterval requireDivisor = relaxedCacheReuse ? null : params.timeUnit;
        TimeRange alignedTimeRange = new TimeRange(params.alignedFrom, params.alignedTo);
        List<CoveredSpan> covered = cache.getCoarsestPerRegionCoverage(
                params.measure, alignedTimeRange, maxAgg, false, requireDivisor);
        if (covered.isEmpty()) return;
        if (io != null) {
            for (CoveredSpan cs : covered) {
                io.recordCacheCovered(cs.getOwnedTo() - cs.getOwnedFrom());
            }
        }
        LOG.info("Coarsest-per-region cover for measure {} ({}): {} regions (maxAgg={}, relaxed={})",
                params.measure, method, covered.size(), maxAgg, relaxedCacheReuse);
        processor.processDatapoints(sketches, covered,
                params.alignedFrom, params.alignedTo, params.timeUnit, relaxedCacheReuse);
    }

    /** Fetch the given grouped intervals, push to cache, and pour into sketches. */
    static void fetchAndIngest(List<Sketch> sketches, TimeSeriesCache cache,
                               PatternDataProcessor processor, PatternMethod method,
                               QueryParams params, int aggFactor,
                               List<TimeInterval> groupedIntervals, IoStats io) {
        Map<Integer, List<TimeSeriesSpan>> fetched = processor.getMissing(
                params.measure, params.alignedFrom, params.alignedTo,
                groupedIntervals, aggFactor, params.timeUnit);
        for (List<TimeSeriesSpan> spans : fetched.values()) {
            if (cache == null) {
                throw new IllegalArgumentException("Cache is null, cannot add spans");
            }
            if (io != null) io.recordCachedFetch(spans);
            cache.addToCache(spans);
            processor.processDatapoints(sketches, spans,
                    params.alignedFrom, params.alignedTo, params.timeUnit);
        }
    }

    /**
     * Refinement is capped so each sub-bucket holds at least {@code
     * dataReductionFactor} raw samples — past that the sub-bucket grid collapses
     * onto the sampling grid and the LP bound stops improving. Shared ceiling for
     * both the query-aware and slope executors.
     */
    static int dataResolutionCap(DataSource dataSource, QueryParams params, int dataReductionFactor) {
        int reductionFactor = Math.max(1, dataReductionFactor);
        long sampleIntervalMs = dataSource.getDataset().getSamplingInterval();
        long timeUnitMs = params.timeUnit.toDuration().toMillis();
        return sampleIntervalMs > 0 && timeUnitMs > 0
                ? (int) Math.max(1, Math.min((long) RefinementPredictor.MAX_AGG_FACTOR,
                        timeUnitMs / ((long) reductionFactor * sampleIntervalMs)))
                : RefinementPredictor.MAX_AGG_FACTOR;
    }

    /**
     * OLS slope field is exact (zero bound width), so the slope-mode ladder
     * degenerates: patch any uninitialised units at the current α and run the
     * strict NFA. Shared by both slope executors.
     */
    static PatternQueryResults olsShortCircuit(List<Sketch> sketches, TimeSeriesCache cache,
                                               PatternDataProcessor processor, PatternEvaluator evaluator,
                                               List<PatternNode> patternNodes, QueryParams params,
                                               int currentAggFactor, long startTime, RefinementStats stats,
                                               IoStats io) {
        List<TimeInterval> missing = PatternDataProcessor.identifyDataMissingIntervals(sketches);
        if (!missing.isEmpty()) {
            List<TimeInterval> grouped = DateTimeUtil.groupIntervals(params.timeUnit, missing);
            fetchAndIngest(sketches, cache, processor, PatternMethod.OLS, params, currentAggFactor, grouped, io);
        }
        List<PatternMatch> matches = evaluator.evaluateStrict(sketches, patternNodes);
        stats.matchesBefore = matches.size();
        stats.matchesAfter = matches.size();
        stats.errorBefore = 0.0;
        stats.errorAfter = 0.0;
        return createResults(matches, startTime, stats);
    }

    static List<PatternMatch> combine(List<PatternMatch> a, List<PatternMatch> b) {
        List<PatternMatch> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    /**
     * Build the result envelope. When {@code stats} is non-null, its refinement
     * fields are copied so downstream consumers get per-query α/error signals.
     * {@code candidateMatches} is attached only when fallback fired (mode-dependent;
     * callers pass null if not applicable).
     */
    static PatternQueryResults createResults(List<PatternMatch> matches,
                                             List<PatternMatch> candidateMatches,
                                             long startTime, RefinementStats stats) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        LOG.info("Pattern query executed in {} ms", executionTime);

        PatternQueryResults results = new PatternQueryResults();
        results.setMatches(matches);
        results.setExecutionTime(executionTime);
        if (stats != null) {
            results.setInitialAggFactor(stats.initialAggFactor);
            results.setFinalAggFactor(stats.finalAggFactor);
            results.setRefinementTriggered(stats.refinementTriggered);
            results.setFallbackTriggered(stats.fallbackTriggered);
            results.setErrorBefore(stats.errorBefore);
            results.setErrorAfter(stats.errorAfter);
            results.setAmbiguousAfter(stats.ambiguousAfter);
        }
        if (results.isFallbackTriggered()) {
            results.setCandidateMatches(candidateMatches);
        }
        return results;
    }

    static PatternQueryResults createResults(List<PatternMatch> matches, long startTime, RefinementStats stats) {
        return createResults(matches, null, startTime, stats);
    }

    /** Helper class for query parameters extracted from a PatternQuery. */
    static final class QueryParams {
        final long alignedFrom;
        final long alignedTo;
        final double accuracy;
        final int measure;
        final AggregateInterval timeUnit;

        QueryParams(long alignedFrom, long alignedTo, double accuracy,
                    int measure, AggregateInterval timeUnit) {
            this.alignedFrom = alignedFrom;
            this.alignedTo = alignedTo;
            this.accuracy = accuracy;
            this.measure = measure;
            this.timeUnit = timeUnit;
        }
    }

    /** Per-query record of how the refinement step behaved. Scoped-mode fields
     *  ({@code fallbackTriggered}, {@code olsVerifiedPromotions}, {@code
     *  ambiguousAfter}, {@code ambBoundWidthDeg}, {@code ambOvershootFrac}) stay
     *  zero / false in full mode. */
    static final class RefinementStats {
        int initialAggFactor;
        int finalAggFactor;
        boolean refinementTriggered = false;
        boolean fallbackTriggered = false;
        int ambiguousAfter = 0;
        int olsVerifiedPromotions = 0;
        int matchesBefore;
        int matchesAfter;
        double errorBefore;
        double errorAfter;
        double ambBoundWidthDeg = 0.0;
        double ambOvershootFrac = 0.0;

        @Override
        public String toString() {
            return String.format(
                    "initialAggFactor=%d finalAggFactor=%d refined=%s fallback=%s "
                            + "matches %d->%d error %.4f->%.4f ambiguousAfter=%d ambBoundWidthDeg=%.3f ambOvershootFrac=%.3f olsVerifiedPromotions=%d",
                    initialAggFactor, finalAggFactor, refinementTriggered, fallbackTriggered,
                    matchesBefore, matchesAfter, errorBefore, errorAfter, ambiguousAfter,
                    ambBoundWidthDeg, ambOvershootFrac, olsVerifiedPromotions);
        }
    }
}
