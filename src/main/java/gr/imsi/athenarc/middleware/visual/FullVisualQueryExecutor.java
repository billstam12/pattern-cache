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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.middleware.cache.AggregationFactorService;
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
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.Stats;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

public class FullVisualQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FullVisualQueryExecutor.class);
    private final DataSource dataSource;
    private final AbstractDataset dataset;
    private final AggregationFactorService aggFactorService;

    private final int initialAggFactor;
    private final int dataReductionFactor;

    protected FullVisualQueryExecutor(DataSource dataSource, int aggFactor) {
        this(dataSource, aggFactor, 4);
    }

    protected FullVisualQueryExecutor(DataSource dataSource, int aggFactor, int dataReductionFactor) {
        this.dataSource = dataSource;
        this.dataset = dataSource.getDataset();
        this.initialAggFactor = aggFactor;
        this.aggFactorService = AggregationFactorService.getInstance();
        this.dataReductionFactor = Math.max(1, dataReductionFactor);
    }

    protected VisualQueryResults executeQuery(VisualQuery query, TimeSeriesCache cache,
                                     DataProcessor dataProcessor, PrefetchManager prefetchManager){        
        // If this is a query with measure-specific aggregate intervals, use that path
        if (query.hasAggregateIntervalsPerMeasure()) {
            LOG.info("Using measure-specific aggregate intervals");
            return executeQueryWithMeasureIntervals(query, dataSource, cache);
        }
        
        double accuracy = query.getAccuracy();
        if(accuracy == 1) return VisualUtils.executeM4Query(query, dataSource);

        // Bound from and to to dataset range
        long from = Math.max(dataset.getTimeRange().getFrom(), query.getFrom());
        long to = Math.min(dataset.getTimeRange().getTo(), query.getTo());
        VisualQueryResults queryResults = new VisualQueryResults();

        ViewPort viewPort = query.getViewPort();

        long pixelColumnIntervalInMillis = (to - from) / viewPort.getWidth();
        AggregateInterval pixelColumnInterval = AggregateInterval.of(pixelColumnIntervalInMillis, ChronoUnit.MILLIS);

        int visDataResolutionCap = VisualUtils.computeVisDataResolutionCap(
                pixelColumnIntervalInMillis, dataset.getSamplingInterval(), dataReductionFactor);
        double queryTime = 0;
        VisualUtils.IoStats io = new VisualUtils.IoStats();
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());
        Map<Integer, List<DataPoint>> resultData = new HashMap<>(measures.size());
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = VisualUtils.initPixelColumns(
                from, viewPort, pixelColumnIntervalInMillis, measures);

        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure = cache.getFromCacheForVisualization(query, pixelColumnInterval);
        // Cache-served coverage → hit-ms. Caveat: overlapping spans (different
        // agg factors over same range) can over-count; see IoStats javadoc.
        for (List<TimeSeriesSpan> cached : overlappingSpansPerMeasure.values()) {
            if (cached == null) continue;
            for (TimeSeriesSpan s : cached) io.recordCacheCovered(s.getTo() - s.getFrom());
        }
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(measures.size());
        Map<Integer, ErrorResults> errorPerMeasure = new HashMap<>(measures.size());
        long aggInterval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
       
        // These is where the pixel columns start and end, as the agg interval is not a float.
        long startPixelColumn = from;
        long endPixelColumn = query.getFrom() + aggInterval * (query.getViewPort().getWidth());

        // Per-query snapshot of (measure → α at this pixelColumnInterval). Built as we
        // walk measures and pass to getMissing/prefetch downstream — they take
        // measure-keyed maps and only ever operate within the current basis.
        Map<Integer, Integer> aggFactorSnapshot = new HashMap<>(measures.size());

        // Per-measure tentative refined α — held back until the post-fetch error
        // confirms the refinement actually got under target. If M4 fallback fires
        // for a measure, we drop its tentative value rather than persist it, so the
        // next query starts from the prior α instead of inheriting a too-tight one.
        Map<Integer, Integer> tentativeRefinedFactor = new HashMap<>(measures.size());

        // Measures we send straight to M4 fallback without a refetch — currently
        // populated when refinement is capped (refetching at the same α would
        // produce identical data; M4 is the real fallback in that case).
        List<Integer> measuresWithError = new ArrayList<>();

        for(int measure : measures){
            // Get overlapping spans
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(measure);

            // Add to pixel columns
            List<PixelColumn> pixelColumns =  pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);

            // Calculate Error
            ErrorResults errorResults = new ErrorResults();
            VisualEvaluator errorCalculator = VisualUtils.evaluateMeasure(
                    pixelColumns, viewPort, pixelColumnInterval, accuracy, errorResults);
            double errorForMeasure = errorResults.getError();
            errorPerMeasure.put(measure, errorResults);
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getInconclusiveIntervals();

            int currentFactor = aggFactorService.getAggFactor(measure, initialAggFactor);

            LOG.info("Partial Error for {} : {}", measure, errorForMeasure);

            if(errorCalculator.hasError()){
                OptionalInt next = RefinementPredictor.nextAggFactor(
                        currentFactor, errorForMeasure, 1.0 - accuracy, visDataResolutionCap);
                if (next.isPresent()) {
                    int newFactor = next.getAsInt();
                    LOG.info("Refinement DOUBLING on visual measure {}: aggFactor {} -> {} (observed={}, target={}; deferred persist)",
                            measure, currentFactor, newFactor, errorForMeasure, 1.0 - accuracy);
                    tentativeRefinedFactor.put(measure, newFactor);
                    currentFactor = newFactor;
                    // Refetch the full range at the refined α to drive error down.
                    missingIntervalsForMeasure = new ArrayList<>();
                    missingIntervalsForMeasure.add(new TimeRange(from, to));
                } else {
                    // Capped: refetching at the same α would re-issue identical SQL
                    // and yield the same error. Send the measure straight to M4
                    // instead of paying for a wasted aggregate fetch.
                    LOG.info("Refinement capped on visual measure {}: aggFactor {} (observed={}, target={}). Routing to M4 fallback.",
                            measure, currentFactor, errorForMeasure, 1.0 - accuracy);
                    measuresWithError.add(measure);
                }
            }

            aggFactorSnapshot.put(measure, currentFactor);

            LOG.debug("Getting {} for measure {}", missingIntervalsForMeasure, measure);
            if(missingIntervalsForMeasure.size() > 0){
                missingIntervalsPerMeasure.put(measure, missingIntervalsForMeasure);
            }
        }

        // Fetch the missing data from the data source.
        // Give the measures with misses, their intervals and their respective agg factors.
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpansPerMeasure = missingIntervalsPerMeasure.size() > 0 ?
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactorSnapshot, viewPort) : new HashMap<>(measures.size());

        // For each measure with a miss, add the fetched data points to the pixel columns and recalculate the error.
        for(int measureWithMiss : missingTimeSeriesSpansPerMeasure.keySet()) {
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measureWithMiss);
            List<TimeSeriesSpan> timeSeriesSpans = missingTimeSeriesSpansPerMeasure.get(measureWithMiss);
            io.recordCachedFetch(timeSeriesSpans);
            // Add to pixel columns
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, timeSeriesSpans);

            // Recalculate error per measure
            ErrorResults errorResults = new ErrorResults();
            VisualEvaluator errorCalculator = VisualUtils.evaluateMeasure(
                    pixelColumns, viewPort, pixelColumnInterval, accuracy, errorResults);
            double errorForMeasure = errorResults.getError();

            LOG.info("Error for {} after refinement: {}", measureWithMiss, errorForMeasure);
            if (errorCalculator.hasError()) {
                LOG.info("Measure {} has error", measureWithMiss);
                measuresWithError.add(measureWithMiss);
                // Refinement at this α did not bring error under target. M4 below
                // satisfies the current query. For the *next* query: persist the
                // predictor's next-refinement step from the failed α — not the
                // failed α itself — so the next query starts already past the
                // level we know is insufficient instead of redoing the same
                // prediction from the prior α every time.
                Integer failedTentative = tentativeRefinedFactor.remove(measureWithMiss);
                if (failedTentative != null) {
                    // If still room to double, advance past the failed α; otherwise
                    // persist the failed α itself so we don't keep re-fetching from
                    // a lower prior.
                    int advanced = RefinementPredictor.nextAggFactor(
                            failedTentative, errorForMeasure, 1.0 - accuracy, visDataResolutionCap)
                            .orElse(failedTentative);
                    aggFactorService.setAggFactor(measureWithMiss, advanced);
                    LOG.info("Refinement on visual measure {} from failed aggFactor={} (error={}); persisting next-refined aggFactor={} for the next query",
                            measureWithMiss, failedTentative, errorForMeasure, advanced);
                }
            } else {
                // Refinement (if any) succeeded — commit the tentative α.
                LOG.info("Refinement succeded");
                Integer committed = tentativeRefinedFactor.remove(measureWithMiss);
                if (committed != null) {
                    aggFactorService.setAggFactor(measureWithMiss, committed);
                }
            }
            errorPerMeasure.put(measureWithMiss, errorResults);
            pixelColumnsPerMeasure.put(measureWithMiss, pixelColumns);
            // Add them all to the cache.
            cache.addToCache(timeSeriesSpans);
        }
        // Fetch errored measures with M4
        if(!measuresWithError.isEmpty()) {
            LOG.info("Error {} cannot be satisfied for measures {}, using M4", errorPerMeasure, measuresWithError);
            VisualQuery m4Query = new VisualQuery(from, to, measuresWithError, viewPort.getWidth(), viewPort.getHeight(), 1.0f);
            VisualQueryResults m4QueryResults = VisualUtils.executeM4Query(m4Query, dataSource);
            long timeStart = System.currentTimeMillis();
            // M4 fallback results are returned to the caller but not cached, so
            // no contribution to IO under the bytes-added-to-cache semantics.
            measuresWithError.forEach(m -> resultData.put(m, m4QueryResults.getData().get(m))); // add m4 results to final result
            // Set error to 0
            ErrorResults errorResults = new ErrorResults();
            measuresWithError.forEach(m -> errorPerMeasure.put(m, errorResults)); // set error to 0;
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
        }

        // Query Results
        List<Integer> measuresWithoutError = new ArrayList<>(measures);
        measuresWithoutError.removeAll(measuresWithError); // remove measures handled with m4 query
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>(measures.size());

        for (int measure : measuresWithoutError) {
            List<DataPoint> dataPoints = new ArrayList<>();
            DoubleSummaryStatistics measureStats = VisualUtils.buildMeasureResult(
                    measure, pixelColumnsPerMeasure.get(measure), Collections.emptyMap(), dataPoints);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetching
        prefetchManager.prefetch(query, aggFactorSnapshot);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        // queryResults.setError(errorPerMeasure);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryResults.setIoCount(io.ioCount());
        queryResults.setCacheHitRatio(io.cacheHitRatio());
        return queryResults;
    }

    private VisualQueryResults executeQueryWithMeasureIntervals(VisualQuery query, DataSource dataSource, TimeSeriesCache cache) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long from = query.getFrom();
        long to = query.getTo();
        VisualQueryResults queryResults = new VisualQueryResults();
        Map<Integer, AggregateInterval> measureIntervals = query.getAggregateIntervalsPerMeasure();
        
        // Prepare intervals for each measure
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        
        for (Integer measure : query.getMeasures()) {
            if (!measureIntervals.containsKey(measure)) {
                LOG.warn("No interval specified for measure {}, skipping", measure);
                continue;
            }
            
            List<TimeInterval> intervals = new ArrayList<>();
            intervals.add(DateTimeUtil.alignIntervalToTimeUnitBoundary(query, measureIntervals.get(measure)));
            intervalsPerMeasure.put(measure, intervals);
        }

        Set<String> aggregateFunctions = AggregationFunctionsConfig.getAggregateFunctions("minMax");
        // Fetch data 
        AggregatedDataPoints dataPoints = dataSource.getAggregatedDataPoints(
            from, to, intervalsPerMeasure, measureIntervals, aggregateFunctions);
        
        // Convert to time series spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
            TimeSeriesSpanFactory.createAggregate(dataPoints, intervalsPerMeasure, measureIntervals, "m4");
        
        // if cache is given, add the spans to it
        for(int measure : timeSeriesSpans.keySet()){
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            cache.addToCache(spans);
        }
        Map<Integer, List<DataPoint>> data = new HashMap<>();
        for (Integer measure : timeSeriesSpans.keySet()) {
            if (!timeSeriesSpans.containsKey(measure)) continue;
            
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPointList = new ArrayList<>();
            
            for (TimeSeriesSpan span : spans) {
                if (span instanceof M4AggregateTimeSeriesSpan) {
                    Iterator<AggregatedDataPoint> it = ((M4AggregateTimeSeriesSpan) span).iterator();
                    while (it.hasNext()) {
                        AggregatedDataPoint point = it.next();
                        Stats stats = point.getStats();
                        // Add key points (first, min, max, last)
                        dataPointList.add(new ImmutableDataPoint(stats.getFirstTimestamp(), stats.getFirstValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getMinTimestamp(), stats.getMinValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getMaxTimestamp(), stats.getMaxValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getLastTimestamp(), stats.getLastValue(), measure));
                        // LOG.info("Added first point: {}-{}, last point: {}-{}, or measure {}",
                        //     stats.getFirstTimestamp(), stats.getFirstValue(), stats.getLastTimestamp(), stats.getLastValue(), measure);
                    }
                }
            }
            data.put(measure, dataPointList);
        }
        stopwatch.stop();
        queryResults.setData(data);
        queryResults.setTimeRange(new TimeRange(query.getFrom(), query.getTo()));
        queryResults.setQueryTime(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
        return queryResults;
    }
}
