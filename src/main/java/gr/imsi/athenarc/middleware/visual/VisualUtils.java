package gr.imsi.athenarc.middleware.visual;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.middleware.cache.M4AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.RawTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

public class VisualUtils {

    /** Per-query IO accountant for cached visual queries, mirroring {@code
     *  PatternUtils.IoStats}. Records cache-served time coverage (hits) and
     *  bytes admitted to the cache from fetches (misses). IO is in <b>bytes</b>
     *  via {@link TimeSeriesSpan#calculateDeepMemorySize()} — same unit as the
     *  {@code Cache Size} column ({@code CacheMemoryManager} accumulates the
     *  same value), so {@code IO Count ≈ ΔCache Size} per query (modulo
     *  eviction).
     *
     *  <p>Caveat: cache-served spans may overlap (different aggregate factors
     *  covering the same range), so {@code hitMs} can over-count vs a true
     *  non-overlapping cover. The visual cache lookup doesn't expose owned
     *  regions the way the pattern path's {@code CoveredSpan} does. */
    public static final class IoStats {
        long hitMs;
        long missMs;
        long ioBytes;

        public void recordCacheCovered(long ms) {
            if (ms > 0) hitMs += ms;
        }

        public void recordCachedFetch(java.util.Collection<? extends gr.imsi.athenarc.middleware.cache.TimeSeriesSpan> spans) {
            for (gr.imsi.athenarc.middleware.cache.TimeSeriesSpan s : spans) {
                long span = s.getTo() - s.getFrom();
                if (span > 0) missMs += span;
                ioBytes += s.calculateDeepMemorySize();
            }
        }

        public long ioCount() { return ioBytes; }

        public double cacheHitRatio() {
            long total = hitMs + missMs;
            return total > 0 ? (double) hitMs / total : 0.0;
        }
    }

    /**
     * Refinement cap mirroring the pattern path's dataResolutionCap: past α =
     * {@code pixelColumnIntervalInMillis / (dataReductionFactor * sampling)},
     * the sub-bucket grid collapses onto the sampling grid and refinement burns
     * IO for no precision gain. Beyond that point, {@code DataProcessor.getMissing}
     * switches to raw fetch. Sampling interval of 0 (unknown rate) → no extra
     * cap beyond {@link RefinementPredictor#MAX_AGG_FACTOR}.
     */
    static int computeVisDataResolutionCap(long pixelColumnIntervalInMillis,
                                           long sampleIntervalMs, int dataReductionFactor) {
        if (sampleIntervalMs <= 0 || pixelColumnIntervalInMillis <= 0) {
            return RefinementPredictor.MAX_AGG_FACTOR;
        }
        return (int) Math.max(1, Math.min(
                (long) RefinementPredictor.MAX_AGG_FACTOR,
                pixelColumnIntervalInMillis / ((long) dataReductionFactor * sampleIntervalMs)));
    }

    /** Initialise per-measure pixel-column lists tiled across [from, from + width × interval]. */
    static Map<Integer, List<PixelColumn>> initPixelColumns(long from, ViewPort viewPort,
                                                            long pixelColumnIntervalInMillis,
                                                            List<Integer> measures) {
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = new HashMap<>(measures.size());
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnIntervalInMillis);
                long pixelTo = pixelFrom + pixelColumnIntervalInMillis;
                pixelColumns.add(new PixelColumn(pixelFrom, pixelTo));
            }
            pixelColumnsPerMeasure.put(measure, pixelColumns);
        }
        return pixelColumnsPerMeasure;
    }

    /**
     * Run the visual error evaluator over a measure's pixel columns and capture the
     * resulting error + false/missing pixels into {@code errResultOut}. Returns the
     * evaluator so callers can read {@code hasError()} / {@code getInconclusiveIntervals()} /
     * {@code getHighErrorIntervals()}.
     */
    static VisualEvaluator evaluateMeasure(List<PixelColumn> pixelColumns, ViewPort viewPort,
                                           AggregateInterval pixelColumnInterval, double accuracy,
                                           ErrorResults errResultOut) {
        VisualEvaluator evaluator = new VisualEvaluator();
        double err = evaluator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, accuracy);
        errResultOut.setError(err);
        errResultOut.setFalsePixels(evaluator.getFalsePixels());
        errResultOut.setMissingPixels(evaluator.getMissingPixels());
        return evaluator;
    }

    /**
     * Build a measure's result series from its pixel columns: each evaluable column
     * (or its M4 override, when {@code overrides} holds an entry for that column index)
     * contributes first/min/max/last points to {@code dataPointsOut}. Returns the
     * column-level min/max/count/sum summary. Pass an empty override map for the
     * full-range path.
     */
    static DoubleSummaryStatistics buildMeasureResult(int measure, List<PixelColumn> pixelColumns,
                                                      Map<Integer, Stats> overrides,
                                                      List<DataPoint> dataPointsOut) {
        int count = 0;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        double sum = 0;
        for (int i = 0; i < pixelColumns.size(); i++) {
            Stats stats = overrides.containsKey(i) ? overrides.get(i) : pixelColumns.get(i).getStats();
            if (stats.getCount() <= 0) continue;
            dataPointsOut.add(new ImmutableDataPoint(stats.getFirstTimestamp(), stats.getFirstValue(), measure));
            dataPointsOut.add(new ImmutableDataPoint(stats.getMinTimestamp(), stats.getMinValue(), measure));
            dataPointsOut.add(new ImmutableDataPoint(stats.getMaxTimestamp(), stats.getMaxValue(), measure));
            dataPointsOut.add(new ImmutableDataPoint(stats.getLastTimestamp(), stats.getLastValue(), measure));
            count++;
            if (max < stats.getMaxValue()) max = stats.getMaxValue();
            if (min > stats.getMinValue()) min = stats.getMinValue();
            sum += stats.getMaxValue() + stats.getMinValue();
        }
        return new DoubleSummaryStatistics(count, min, max, sum);
    }


    public static VisualQueryResults executeRawQuery(VisualQuery query, DataSource dataSource){
        VisualQueryResults queryResults = new VisualQueryResults();

        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, List<DataPoint>> rawData = new HashMap<>();
        for(int measure : query.getMeasures()){
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getTo()));
            missingIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), missingIntervalsPerMeasure);
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createRaw(dataPoints, missingIntervalsPerMeasure);
        long ioCount = 0;
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPointsForMeasure = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                // IO in bytes — same unit as cached paths' deepMemorySize.
                ioCount += span.calculateDeepMemorySize();
                Iterator<DataPoint> it = ((RawTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    DataPoint dataPoint = it.next();
                    // The raw span iterator is a flyweight — copy before retaining.
                    dataPointsForMeasure.add(new ImmutableDataPoint(
                            dataPoint.getTimestamp(), dataPoint.getValue(), measure));
                }
            }
            rawData.put(measure, dataPointsForMeasure);
        }
        queryResults.setData(rawData);
        queryResults.setIoCount(ioCount);
        return queryResults;
    }
    public static VisualQueryResults executeM4Query(VisualQuery query, DataSource dataSource) {
        VisualQueryResults queryResults = new VisualQueryResults();
        Map<Integer, List<DataPoint>> m4Data = new HashMap<>();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, AggregateInterval> aggregateIntervals = new HashMap<>(query.getMeasures().size());

        long interval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
        AggregateInterval aggInterval = AggregateInterval.of(interval, ChronoUnit.MILLIS);
        long startPixelColumn = query.getFrom();
        long endPixelColumn = query.getFrom() + interval * (query.getViewPort().getWidth());

        for (Integer measure : query.getMeasures()) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getFrom() + interval * (query.getViewPort().getWidth())));
            missingIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            aggregateIntervals.put(measure, aggInterval);
        }

        AggregatedDataPoints missingDataPoints = 
            dataSource.getAggregatedDataPointsWithTimestamps(startPixelColumn, endPixelColumn, missingIntervalsPerMeasure, aggregateIntervals, AggregationFunctionsConfig.getAggregateFunctions("m4"));
        
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingIntervalsPerMeasure, aggregateIntervals, "m4");
        long ioCount = 0;
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPoints = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                // IO in bytes — same unit as cached paths' deepMemorySize.
                ioCount += span.calculateDeepMemorySize();
                Iterator<AggregatedDataPoint> it = ((M4AggregateTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = it.next();
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getFirstDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getFirstDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMinDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMinDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMaxDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMaxDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getLastDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getLastDataPoint().getValue(), measure));

                }
            }
            m4Data.put(measure, dataPoints);
        }
        Map<Integer, ErrorResults> error = new HashMap<>();
        for(Integer m : query.getMeasures()){
            error.put(m, new ErrorResults());
        }
        queryResults.setData(m4Data);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        queryResults.setQueryTime(queryTime);
        queryResults.setIoCount(ioCount);
        return queryResults;
    }
}
