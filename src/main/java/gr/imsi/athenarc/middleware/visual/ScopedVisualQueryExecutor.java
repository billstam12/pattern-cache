package gr.imsi.athenarc.middleware.visual;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.cache.M4AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

/**
 * Scoped visual executor — refetches only the regions that need it instead of
 * the whole query range, mirroring the pattern path's scoped refinement. The
 * fetch ladder is: patch uninitialised pixel columns at the current α (no
 * error eval yet, since uninitialised columns can't be evaluated), then a
 * first error evaluation, then refetch high-error columns at the refined α,
 * then a scoped M4 fallback over any columns still in error.
 *
 * <p>Up to three fetches per measure vs. {@link FullVisualQueryExecutor}'s
 * single full-range refetch + full-range M4. The trade-off is more roundtrips
 * for less bytes when high-error and data-missing intervals are localised.
 */
public class ScopedVisualQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScopedVisualQueryExecutor.class);
    private final DataSource dataSource;
    private final AbstractDataset dataset;

    private final int initialAggFactor;
    private final int dataReductionFactor;
    /** Max number of doubling refinements per measure before falling through to
     *  the scoped M4 fallback. Higher = more chances to resolve via finer α
     *  aggregates; lower = fewer roundtrips but earlier M4 fallback. */
    private final int maxRefinementSteps;

    /** Used to handle the measure-specific-intervals path (M4 special case)
     *  without re-implementing it — the scope dispatch is orthogonal to that
     *  query shape. */
    private final FullVisualQueryExecutor measureIntervalsDelegate;

    protected ScopedVisualQueryExecutor(DataSource dataSource, int aggFactor) {
        this(dataSource, aggFactor, 4, 20);
    }

    protected ScopedVisualQueryExecutor(DataSource dataSource, int aggFactor, int dataReductionFactor) {
        this(dataSource, aggFactor, dataReductionFactor, 20);
    }

    protected ScopedVisualQueryExecutor(DataSource dataSource, int aggFactor, int dataReductionFactor,
                                        int maxRefinementSteps) {
        this.dataSource = dataSource;
        this.dataset = dataSource.getDataset();
        this.initialAggFactor = aggFactor;
        this.dataReductionFactor = Math.max(1, dataReductionFactor);
        this.maxRefinementSteps = Math.max(0, maxRefinementSteps);
        this.measureIntervalsDelegate = new FullVisualQueryExecutor(dataSource, aggFactor, dataReductionFactor);
    }

    protected VisualQueryResults executeQuery(VisualQuery query, TimeSeriesCache cache,
                                     DataProcessor dataProcessor, PrefetchManager prefetchManager) {
        if (query.hasAggregateIntervalsPerMeasure()) {
            LOG.info("Measure-specific aggregate intervals: delegating to baseline executor (scope is no-op for this query shape)");
            return measureIntervalsDelegate.executeQuery(query, cache, dataProcessor, prefetchManager);
        }

        double accuracy = query.getAccuracy();
        if (accuracy == 1) return VisualUtils.executeM4Query(query, dataSource);
        double targetSlack = 1.0 - accuracy;

        long from = Math.max(dataset.getTimeRange().getFrom(), query.getFrom());
        long to = Math.min(dataset.getTimeRange().getTo(), query.getTo());
        VisualQueryResults queryResults = new VisualQueryResults();

        ViewPort viewPort = query.getViewPort();
        long pixelColumnIntervalInMillis = (to - from) / viewPort.getWidth();
        AggregateInterval pixelColumnInterval = AggregateInterval.of(pixelColumnIntervalInMillis, ChronoUnit.MILLIS);

        int visDataResolutionCap = VisualUtils.computeVisDataResolutionCap(
                pixelColumnIntervalInMillis, dataset.getSamplingInterval(), dataReductionFactor);

        VisualUtils.IoStats io = new VisualUtils.IoStats();
        Stopwatch stopwatch = Stopwatch.createStarted();

        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());
        Map<Integer, List<DataPoint>> resultData = new HashMap<>(measures.size());

        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = VisualUtils.initPixelColumns(
                from, viewPort, pixelColumnIntervalInMillis, measures);

        long startPixelColumn = from;
        long aggInterval = (query.getTo() - query.getFrom()) / viewPort.getWidth();
        long endPixelColumn = query.getFrom() + aggInterval * viewPort.getWidth();

        Map<Integer, ErrorResults> errorPerMeasure = new HashMap<>(measures.size());
        Map<Integer, VisualEvaluator> evaluatorPerMeasure = new HashMap<>(measures.size());
        Map<Integer, Integer> currentAggFactorPerMeasure = new HashMap<>(measures.size());

        // Cache load + identify data-missing intervals. Uninitialised pixel
        // columns are the cache-miss regions — pure post-load state, no error
        // eval yet (error can't be evaluated until the data is there).
        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure =
                cache.getFromCacheForVisualization(query, pixelColumnInterval);
        // Cache-served coverage → hit-ms. Caveat: overlapping spans (different
        // agg factors over same range) can over-count; see IoStats javadoc.
        for (List<TimeSeriesSpan> cached : overlappingSpansPerMeasure.values()) {
            if (cached == null) continue;
            for (TimeSeriesSpan s : cached) io.recordCacheCovered(s.getTo() - s.getFrom());
        }
        Map<Integer, List<TimeInterval>> dataMissingIntervals = new HashMap<>();
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns,
                    overlappingSpansPerMeasure.get(measure));
            List<TimeInterval> missing = identifyMissingPixelColumnIntervals(
                    pixelColumns, pixelColumnInterval);
            if (!missing.isEmpty()) {
                dataMissingIntervals.put(measure, missing);
            }
            // Each query enters at the configured initial α. We don't carry a
            // global per-measure α forward — see ScopedPatternQueryExecutor for
            // the rationale. Refinement work persists in the cache (previously-
            // fetched fine spans get admitted by tighter cover-caps when needed),
            // not via a sidecar ratchet that invalidates the coarse cache.
            currentAggFactorPerMeasure.put(measure, Math.max(1, initialAggFactor));
        }

        // Patch data-missing intervals at the current α — still no error eval.
        if (!dataMissingIntervals.isEmpty()) {
            LOG.info("Patching data-missing for measures {} at current α", dataMissingIntervals.keySet());
            Map<Integer, Integer> dataMissingFactors = sliceFactors(currentAggFactorPerMeasure, dataMissingIntervals.keySet());
            Map<Integer, List<TimeSeriesSpan>> fetched = dataProcessor.getMissing(
                    from, to, dataMissingIntervals, dataMissingFactors, viewPort);
            applyFetchedSpans(fetched, pixelColumnsPerMeasure, dataProcessor, cache,
                    from, to, viewPort, io);
        }

        // First error evaluation — this is where we learn the per-measure error.
        for (int measure : measures) {
            ErrorResults errResult = new ErrorResults();
            VisualEvaluator evaluator = VisualUtils.evaluateMeasure(
                    pixelColumnsPerMeasure.get(measure), viewPort, pixelColumnInterval, accuracy, errResult);
            evaluatorPerMeasure.put(measure, evaluator);
            errorPerMeasure.put(measure, errResult);
        }
        LOG.info("Initial error per measure: {}", errorPerMeasure);

        // Iterative refinement: walk the α ladder up to {@code maxRefinementSteps}
        // times. Each step fetches finer aggregates only over the still-high-error
        // intervals (per measure) and re-evaluates. Measures that drop under
        // target stop refining; measures that hit the resolution cap stop and
        // get picked up by the M4 fallback below.
        Map<Integer, Integer> refinedAggFactorPerMeasure = new HashMap<>(currentAggFactorPerMeasure);
        for (int step = 0; step < maxRefinementSteps; step++) {
            Map<Integer, List<TimeInterval>> highErrorIntervals = new HashMap<>();
            Map<Integer, Integer> stepAggFactor = new HashMap<>();
            for (int measure : measures) {
                VisualEvaluator evaluator = evaluatorPerMeasure.get(measure);
                if (!evaluator.hasError()) continue;
                double errorForMeasure = errorPerMeasure.get(measure).getError();
                int currentFactor = refinedAggFactorPerMeasure.get(measure);
                OptionalInt next = RefinementPredictor.nextAggFactor(
                        currentFactor, errorForMeasure, targetSlack, visDataResolutionCap);
                if (next.isPresent() && next.getAsInt() != currentFactor) {
                    int newFactor = next.getAsInt();
                    LOG.info("Refining measure {} (step {}/{}): α {} → {} (error={}, target={})",
                            measure, step + 1, maxRefinementSteps,
                            currentFactor, newFactor, errorForMeasure, targetSlack);
                    refinedAggFactorPerMeasure.put(measure, newFactor);
                    stepAggFactor.put(measure, newFactor);
                    highErrorIntervals.put(measure, evaluator.getHighErrorIntervals(targetSlack));
                } else {
                    LOG.info("Refinement capped on measure {} at α={} (error={}); will fall through to M4",
                            measure, currentFactor, errorForMeasure);
                }
            }
            if (highErrorIntervals.isEmpty()) break;
            LOG.info("Refining step {}/{} for measures {} at refined α",
                    step + 1, maxRefinementSteps, highErrorIntervals.keySet());
            Map<Integer, List<TimeSeriesSpan>> fetched = dataProcessor.getMissing(
                    from, to, highErrorIntervals, stepAggFactor, viewPort);
            applyFetchedSpansAndReevaluate(fetched, pixelColumnsPerMeasure, evaluatorPerMeasure,
                    errorPerMeasure, dataProcessor, cache, from, to, viewPort,
                    pixelColumnInterval, accuracy, query, io);
        }

        // Scoped M4 fallback over columns still in error after refinement.
        Map<Integer, List<TimeInterval>> m4FallbackIntervals = new HashMap<>();
        for (int measure : measures) {
            VisualEvaluator evaluator = evaluatorPerMeasure.get(measure);
            if (evaluator.hasError()) {
                m4FallbackIntervals.put(measure, evaluator.getHighErrorIntervals(targetSlack));
            }
        }
        Map<Integer, Map<Integer, Stats>> m4OverridesPerMeasure = Collections.emptyMap();
        if (!m4FallbackIntervals.isEmpty()) {
            LOG.info("Error {} cannot be satisfied for measures {}, using scoped M4",
                    errorPerMeasure, m4FallbackIntervals.keySet());
            long m4Start = System.currentTimeMillis();
            m4OverridesPerMeasure = fetchM4ColumnOverrides(m4FallbackIntervals, pixelColumnInterval,
                    startPixelColumn, endPixelColumn);
            // M4 fallback fetches per-column overrides but does NOT admit the
            // resulting spans to the cache (they're consumed locally), so no
            // contribution to IO under the bytes-added-to-cache semantics.
            ErrorResults zero = new ErrorResults();
            m4FallbackIntervals.keySet().forEach(m -> errorPerMeasure.put(m, zero));
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - m4Start) / 1000F);
        }

        // Build result: M4 overrides (where present) replace pixel-column stats.
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>(measures.size());
        for (int measure : measures) {
            Map<Integer, Stats> overrides = m4OverridesPerMeasure.getOrDefault(
                    measure, Collections.emptyMap());
            List<DataPoint> dataPoints = new ArrayList<>();
            DoubleSummaryStatistics measureStats = VisualUtils.buildMeasureResult(
                    measure, pixelColumnsPerMeasure.get(measure), overrides, dataPoints);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        double queryTime = stopwatch.elapsed(java.util.concurrent.TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetch using the most recent per-measure α (refined if a refinement step fired, else current).
        Map<Integer, Integer> finalAggFactorSnapshot = new HashMap<>(measures.size());
        for (int measure : measures) {
            finalAggFactorSnapshot.put(measure, refinedAggFactorPerMeasure.getOrDefault(
                    measure, currentAggFactorPerMeasure.get(measure)));
        }
        prefetchManager.prefetch(query, finalAggFactorSnapshot);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryResults.setIoCount(io.ioCount());
        queryResults.setCacheHitRatio(io.cacheHitRatio());
        return queryResults;
    }

    /** Pixel-column ranges where the column wasn't touched by any cache span —
     *  the visual analogue of pattern's {@code !sketch.hasInitialized()} check.
     *  Adjacent uninitialised columns are grouped on the {@code pixelColumnInterval}
     *  grid via {@link DateTimeUtil#groupIntervals}. */
    private List<TimeInterval> identifyMissingPixelColumnIntervals(
            List<PixelColumn> pixelColumns, AggregateInterval pixelColumnInterval) {
        List<TimeInterval> missing = new ArrayList<>();
        for (PixelColumn pc : pixelColumns) {
            if (!pc.hasInitialized()) {
                missing.add(new TimeRange(pc.getFrom(), pc.getTo()));
            }
        }
        return DateTimeUtil.groupIntervals(pixelColumnInterval, missing);
    }

    /** Apply fetched spans to each measure's pixel columns and push to cache.
     *  Used by the data-missing patch step — no re-eval, since the first error
     *  evaluation comes after this. Records bytes admitted + miss-ms into {@code io}. */
    private void applyFetchedSpans(
            Map<Integer, List<TimeSeriesSpan>> fetched,
            Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure,
            DataProcessor dataProcessor, TimeSeriesCache cache,
            long from, long to, ViewPort viewPort, VisualUtils.IoStats io) {
        for (Map.Entry<Integer, List<TimeSeriesSpan>> e : fetched.entrySet()) {
            List<TimeSeriesSpan> spans = e.getValue();
            io.recordCachedFetch(spans);
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(e.getKey());
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, spans);
            cache.addToCache(spans);
        }
    }

    /** Apply fetched spans + re-evaluate the affected measures. Used by the
     *  refinement step where re-eval is needed to decide if M4 fallback fires.
     *  Records bytes admitted + miss-ms into {@code io}. */
    private void applyFetchedSpansAndReevaluate(
            Map<Integer, List<TimeSeriesSpan>> fetched,
            Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure,
            Map<Integer, VisualEvaluator> evaluatorPerMeasure,
            Map<Integer, ErrorResults> errorPerMeasure,
            DataProcessor dataProcessor, TimeSeriesCache cache,
            long from, long to, ViewPort viewPort,
            AggregateInterval pixelColumnInterval, double accuracy,
            VisualQuery query, VisualUtils.IoStats io) {
        for (Map.Entry<Integer, List<TimeSeriesSpan>> e : fetched.entrySet()) {
            int measure = e.getKey();
            List<TimeSeriesSpan> spans = e.getValue();
            io.recordCachedFetch(spans);
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, spans);
            cache.addToCache(spans);
            ErrorResults errResult = new ErrorResults();
            VisualEvaluator evaluator = VisualUtils.evaluateMeasure(
                    pixelColumns, viewPort, pixelColumnInterval, accuracy, errResult);
            evaluatorPerMeasure.put(measure, evaluator);
            errorPerMeasure.put(measure, errResult);
            LOG.info("Error for {} after refinement: {}", measure, errResult.getError());
        }
    }

    private Map<Integer, Integer> sliceFactors(Map<Integer, Integer> factors, java.util.Set<Integer> keys) {
        Map<Integer, Integer> out = new HashMap<>(keys.size());
        for (int k : keys) out.put(k, factors.get(k));
        return out;
    }

    /**
     * Fetch M4 at {@code pixelColumnInterval} granularity over the given per-measure
     * intervals and return per-column Stats overrides keyed by column index
     * (0-based offset from {@code startPixelColumn}). One M4 bucket per pixel column
     * produces one Stats entry; the result-build loop consumes these to replace
     * pixel-column-derived points on a per-column basis.
     */
    private Map<Integer, Map<Integer, Stats>> fetchM4ColumnOverrides(
            Map<Integer, List<TimeInterval>> intervalsPerMeasure,
            AggregateInterval pixelColumnInterval,
            long startPixelColumn, long endPixelColumn) {
        if (intervalsPerMeasure.isEmpty()) return Collections.emptyMap();
        Map<Integer, AggregateInterval> aggIntervals = new HashMap<>(intervalsPerMeasure.size());
        for (int m : intervalsPerMeasure.keySet()) {
            aggIntervals.put(m, pixelColumnInterval);
        }
        AggregatedDataPoints m4 = dataSource.getAggregatedDataPointsWithTimestamps(
                startPixelColumn, endPixelColumn, intervalsPerMeasure, aggIntervals,
                AggregationFunctionsConfig.getAggregateFunctions("m4"));
        Map<Integer, List<TimeSeriesSpan>> spansPerMeasure = TimeSeriesSpanFactory.createAggregate(
                m4, intervalsPerMeasure, aggIntervals, "m4");
        long pixelColumnIntervalMs = pixelColumnInterval.toDuration().toMillis();
        Map<Integer, Map<Integer, Stats>> overrides = new HashMap<>(spansPerMeasure.size());
        for (Map.Entry<Integer, List<TimeSeriesSpan>> entry : spansPerMeasure.entrySet()) {
            Map<Integer, Stats> perColumn = new HashMap<>();
            for (TimeSeriesSpan span : entry.getValue()) {
                Iterator<AggregatedDataPoint> it = ((M4AggregateTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    AggregatedDataPoint dp = it.next();
                    int columnIdx = (int) ((dp.getFrom() - startPixelColumn) / pixelColumnIntervalMs);
                    perColumn.put(columnIdx, dp.getStats());
                }
            }
            overrides.put(entry.getKey(), perColumn);
        }
        return overrides;
    }
}
