package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.CacheUtils;
import gr.imsi.athenarc.middleware.cache.CoveredSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.sketch.SketchUtils;

/**
 * Pattern-side data ingest + DB fetch. Mirror of visual's {@code DataProcessor}:
 * pours spans into sketches and fetches missing sub-bucket aggregates from the
 * data source. Cache reads/writes stay in the executor — the processor returns
 * spans; the executor decides what to put in the cache and what to populate from.
 */
public class PatternDataProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PatternDataProcessor.class);

    private final DataSource dataSource;
    private final PatternMethod method;
    private final boolean adaptation;
    private final boolean calendarAlignment;

    public PatternDataProcessor(DataSource dataSource, PatternMethod method,
                                boolean adaptation, boolean calendarAlignment) {
        this.dataSource = dataSource;
        this.method = method;
        this.adaptation = adaptation;
        this.calendarAlignment = calendarAlignment;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /** Pour fetched spans into sketches. */
    public void processDatapoints(List<Sketch> sketches, List<TimeSeriesSpan> spans,
                                  long from, long to, AggregateInterval timeUnit) {
        SketchUtils.populateSketchesFromSpans(spans, sketches, from, to, timeUnit);
    }

    /**
     * Pour cache coarsest-per-region spans into sketches. Per-sub-range owned
     * range filtering is enforced by {@code SketchUtils}; under
     * {@code relaxedCacheReuse}, straddler routing applies.
     */
    public void processDatapoints(List<Sketch> sketches, List<CoveredSpan> covered,
                                  long from, long to, AggregateInterval timeUnit,
                                  boolean relaxedCacheReuse) {
        SketchUtils.populateSketchesFromCoveredSpans(covered, sketches, from, to, timeUnit, relaxedCacheReuse);
    }

    /** Pour an {@code AggregatedDataPoints} stream directly (no span wrapping).
     *  Used by the scoped strict-OLS replay. */
    public void processDatapoints(List<Sketch> sketches, AggregatedDataPoints dataPoints,
                                  long from, long to, AggregateInterval timeUnit) {
        SketchUtils.populateSketchesFromDataPoints(dataPoints.iterator(), sketches, from, to, timeUnit);
    }

    /**
     * Sub-bucket size for an APPROX_OLS query at the given α, or {@code timeUnit}
     * for OLS (full rollup). Shared by cache-side and fetch-side so both agree
     * on what compatibility means at this resolution.
     */
    public AggregateInterval computeSubInterval(AggregateInterval timeUnit, int aggFactor) {
        int divider = 1;
        if (method == PatternMethod.APPROX_OLS) {
            divider = adaptation ? Math.max(1, aggFactor) : 4;
        }
        long rawSubIntervalMs = timeUnit.toDuration().toMillis() / divider;
        return calendarAlignment
                ? DateTimeUtil.roundDownToCalendarBasedInterval(rawSubIntervalMs)
                : AggregateInterval.fromMillis(rawSubIntervalMs);
    }

    /**
     * Fetch the given grouped intervals at the per-α sub-bucket resolution and
     * return the resulting spans per measure. Mirrors visual's
     * {@code getMissing(...)} call shape. Caller decides which intervals to
     * fetch (initial-pass missing, refinement-pass ambiguous regions, ...) and
     * groups them.
     */
    public Map<Integer, List<TimeSeriesSpan>> getMissing(int measure, long from, long to,
                                                         List<TimeInterval> groupedIntervals,
                                                         int aggFactor, AggregateInterval timeUnit) {
        if (groupedIntervals == null || groupedIntervals.isEmpty()) {
            return new HashMap<>();
        }
        AggregateInterval subInterval = computeSubInterval(timeUnit, aggFactor);
        LOG.info("Fetch: {} grouped intervals at subInterval={} (aggFactor={})",
                groupedIntervals.size(), subInterval, aggFactor);

        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        intervalsPerMeasure.put(measure, groupedIntervals);

        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, subInterval);

        Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure =
                DateTimeUtil.alignIntervalsToTimeUnitBoundary(intervalsPerMeasure, aggregateIntervalsPerMeasure);

        return CacheUtils.fetchTimeSeriesSpans(dataSource, from, to,
                alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, method.canonical());
    }

    /**
     * Slope-only fetch over the given grouped intervals at the outer-bucket
     * ({@code timeUnit}) resolution. Used by the scoped strict-OLS replay;
     * bypasses the cache because the resulting Slope spans aren't reusable on
     * the normal MinMax path.
     */
    public AggregatedDataPoints getSlopeAggregates(int measure, long from, long to,
                                                   List<TimeInterval> groupedIntervals,
                                                   AggregateInterval timeUnit) {
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        intervalsPerMeasure.put(measure, groupedIntervals);
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure =
                DateTimeUtil.alignIntervalsToTimeUnitBoundary(intervalsPerMeasure, aggregateIntervalsPerMeasure);
        return dataSource.getSlopeAggregates(
                from, to, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, false);
    }

    /** Direct datasource fetch + populate (no cache). Used by the no-cache
     *  pattern path. */
    public void populateFromDataSource(List<Sketch> sketches, int measure,
                                       long from, long to, AggregateInterval timeUnit,
                                       boolean includeMinMax) {
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        List<TimeInterval> intervals = new ArrayList<>();
        intervals.add(new TimeRange(from, to));
        intervalsPerMeasure.put(measure, intervals);
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        AggregatedDataPoints dataPoints;
        switch (method) {
            case OLS:
                dataPoints = dataSource.getSlopeAggregates(
                        from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure, includeMinMax);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method for no-cache pattern query: " + method);
        }
        SketchUtils.populateSketchesFromDataPoints(dataPoints.iterator(), sketches, from, to, timeUnit);
    }

    /**
     * Sketches whose data is still missing (uninitialised) after a cache
     * populate. Pure post-ingest cache-miss detection — visual analog is
     * detecting pixel columns where {@code !hasInitialized()}.
     */
    public static List<TimeInterval> identifyDataMissingIntervals(List<Sketch> sketches) {
        List<TimeInterval> missing = new ArrayList<>();
        for (Sketch sketch : sketches) {
            if (!sketch.hasInitialized()) {
                missing.add(sketch);
            }
        }
        LOG.info("Identified {} missing intervals", missing.size());
        return missing;
    }
}
