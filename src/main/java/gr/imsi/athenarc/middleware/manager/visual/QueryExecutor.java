package gr.imsi.athenarc.middleware.manager.visual;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
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

public class QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);
    private final DataSource dataSource;
    private final AbstractDataset dataset;
    private final Map<Integer, Integer> aggFactors;

    private final int initialAggFactor;

    protected QueryExecutor(DataSource dataSource, int aggFactor) {
        this.dataSource = dataSource;
        this.dataset = dataSource.getDataset();
        this.aggFactors = new HashMap<>(dataset.getMeasures().size());
        this.initialAggFactor = aggFactor;
        for(int measure : dataset.getMeasures()) aggFactors.put(measure, aggFactor);
    }

    void updateAggFactor(int measure){
        int prevAggFactor = aggFactors.get(measure);
        aggFactors.put(measure, prevAggFactor * 2);
    }

    protected VisualQueryResults executeQuery(VisualQuery query, TimeSeriesCache cache,
                                     DataProcessor dataProcessor, PrefetchManager prefetchManager){
        LOG.info("Executing Visual Query {}", query);
        
        // If this is a query with measure-specific aggregate intervals, use that path
        if (query.hasMeasureAggregateIntervals()) {
            LOG.info("Using measure-specific aggregate intervals");
            return executeQueryWithMeasureIntervals(query, cache);
        }
        
        if(query.getAccuracy() == 1) return executeM4Query(query);

        // Bound from and to to dataset range
        long from = Math.max(dataset.getTimeRange().getFrom(), query.getFrom());
        long to = Math.min(dataset.getTimeRange().getTo(), query.getTo());
        VisualQueryResults queryResults = new VisualQueryResults();

        ViewPort viewPort = query.getViewPort();

        long pixelColumnIntervalInMillis = (to - from) / viewPort.getWidth();
        AggregateInterval pixelColumnInterval = AggregateInterval.of(pixelColumnIntervalInMillis, ChronoUnit.MILLIS);
        double queryTime = 0;
        long ioCount = 0;
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());
        Map<Integer, List<DataPoint>> resultData = new HashMap<>(measures.size());
        // Initialize Pixel Columns
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = new HashMap<>(measures.size()); // Lists of pixel columns. One list for every measure.
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnInterval.toDuration().toMillis());
                long pixelTo = pixelFrom + pixelColumnInterval.toDuration().toMillis();
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, viewPort);
                pixelColumns.add(pixelColumn);
            }
            pixelColumnsPerMeasure.put(measure, pixelColumns);
        }

        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure = cache.getFromCacheForVisualization(query, pixelColumnInterval);
        LOG.debug("Overlapping intervals per measure {}", overlappingSpansPerMeasure);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(measures.size());
        Map<Integer, ErrorResults> errorPerMeasure = new HashMap<>(measures.size());
        long aggInterval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
       
        // These is where the pixel columns start and end, as the agg interval is not a float.
        long startPixelColumn = from;
        long endPixelColumn = query.getFrom() + aggInterval * (query.getViewPort().getWidth());

        // For each measure, get the overlapping spans, add them to pixel columns and calculate the error
        // Compute the aggFactor, and if there is an error double it.
        // Finally, add the measure as missing and flag its missing intervals.
        for(int measure : measures){
            // Get overlapping spans
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(measure);

            // Add to pixel columns
            List<PixelColumn> pixelColumns =  pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);

            // Calculate Error
            ErrorCalculator errorCalculator = new ErrorCalculator();
            ErrorResults errorResults = new ErrorResults();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            errorResults.setError(errorForMeasure);
            errorResults.setFalsePixels(errorCalculator.getFalsePixels());
            errorResults.setMissingPixels(errorCalculator.getMissingPixels());
            errorPerMeasure.put(measure, errorResults);
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();

            // Calculate aggFactor
            double coveragePercentages = 0.0;
            double totalAggFactors = 0.0;
            for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
                long size = overlappingSpan.getAggregateInterval().toDuration().toMillis(); // ms
                if(size <= dataset.getSamplingInterval()) continue; // if raw data continue
                double coveragePercentage = overlappingSpan.percentage(query); // coverage
                int spanAggFactor = (int) ((double) (pixelColumnInterval.toDuration().toMillis()) / size);
                totalAggFactors += coveragePercentage * spanAggFactor;
                coveragePercentages += coveragePercentage;
            }
            // The missing intervals get a value equal to the initial value
            for(TimeInterval missingInterval : missingIntervalsForMeasure){
                double coveragePercentage = missingInterval.percentage(query); // coverage
                totalAggFactors += coveragePercentage * initialAggFactor;
                coveragePercentages += coveragePercentage;
            }
            int meanWeightAggFactor = coveragePercentages != 0 ? (int) Math.ceil(totalAggFactors / coveragePercentages) : aggFactors.get(measure);
            aggFactors.put(measure, meanWeightAggFactor);
            // Update aggFactor if there is an error
            if(errorCalculator.hasError()){
                updateAggFactor(measure);
                // Initialize ranges and measures to get all errored data.
                missingIntervalsForMeasure = new ArrayList<>();
                missingIntervalsForMeasure.add(new TimeRange(from, to));
            }
            LOG.debug("Getting {} for measure {}", missingIntervalsForMeasure, measure);
            if(missingIntervalsForMeasure.size() > 0){
                missingIntervalsPerMeasure.put(measure, missingIntervalsForMeasure);
            }
        }
        
        LOG.debug("Errors: {}", errorPerMeasure);
        LOG.info("Agg factors: {}", aggFactors);

        // Fetch the missing data from the data source.
        // Give the measures with misses, their intervals and their respective agg factors.
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpansPerMeasure = missingIntervalsPerMeasure.size() > 0 ?
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactors, viewPort) : new HashMap<>(measures.size());

        List<Integer> measuresWithError = new ArrayList<>();
        // For each measure with a miss, add the fetched data points to the pixel columns and recalculate the error.
        for(int measureWithMiss : missingTimeSeriesSpansPerMeasure.keySet()) {
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measureWithMiss);
            List<TimeSeriesSpan> timeSeriesSpans = missingTimeSeriesSpansPerMeasure.get(measureWithMiss);
            ioCount += timeSeriesSpans.stream().mapToLong(TimeSeriesSpan::getCount).sum();
            // Add to pixel columns
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, timeSeriesSpans);

            // Recalculate error per measure
            ErrorCalculator errorCalculator = new ErrorCalculator();
            ErrorResults errorResults = new ErrorResults();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());

            if (errorCalculator.hasError()) measuresWithError.add(measureWithMiss);
            errorResults.setError(errorForMeasure);
            errorResults.setFalsePixels(errorCalculator.getFalsePixels());
            errorResults.setMissingPixels(errorCalculator.getMissingPixels());

            errorPerMeasure.put(measureWithMiss, errorResults);
            pixelColumnsPerMeasure.put(measureWithMiss, pixelColumns);
            // Add them all to the cache.
            cache.addToCache(timeSeriesSpans);
        }
        // Fetch errored measures with M4
        if(!measuresWithError.isEmpty()) {
            VisualQuery m4Query = new VisualQuery(from, to, measuresWithError, viewPort.getWidth(), viewPort.getHeight(), 1.0f);
            VisualQueryResults m4QueryResults = executeM4Query(m4Query);
            long timeStart = System.currentTimeMillis();
            ioCount += 4 * viewPort.getWidth() * measuresWithError.size();
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
            int count = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);

            List<DataPoint> dataPoints = new ArrayList<>();
            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount() <= 0) {
                    continue;
                }
                // add points
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue(), measure));
                
                // compute statistics
                count += 1;
                if(max < pixelColumnStats.getMaxValue()) max = pixelColumnStats.getMaxValue();
                if(min > pixelColumnStats.getMinValue()) min = pixelColumnStats.getMinValue();
                sum += pixelColumnStats.getMaxValue() + pixelColumnStats.getMinValue();
            }
            DoubleSummaryStatistics measureStats = new
                    DoubleSummaryStatistics(count, min, max, sum);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetching
        prefetchManager.prefetch(query, aggFactors);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        // queryResults.setError(errorPerMeasure);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryResults.setIoCount(ioCount);
        return queryResults;
    }
    
    /**
     * Execute a query with measure-specific aggregate intervals, typically used for cache initialization.
     */
    private VisualQueryResults executeQueryWithMeasureIntervals(VisualQuery query, TimeSeriesCache cache) {
        long from = query.getFrom();
        long to = query.getTo();
        VisualQueryResults queryResults = new VisualQueryResults();
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, AggregateInterval> measureIntervals = query.getMeasureAggregateIntervals();
        
        // Prepare intervals for each measure
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        
        for (Integer measure : query.getMeasures()) {
            if (!measureIntervals.containsKey(measure)) {
                LOG.warn("No interval specified for measure {}, skipping", measure);
                continue;
            }
            
            List<TimeInterval> intervals = new ArrayList<>();
            intervals.add(new TimeRange(from, to));
            intervalsPerMeasure.put(measure, intervals);
        }
        
        // Fetch data 
        AggregatedDataPoints dataPoints = dataSource.getAggregatedDataPoints(
            from, to, intervalsPerMeasure, measureIntervals);
        
        // Convert to time series spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
            TimeSeriesSpanFactory.createAggregate(dataPoints, intervalsPerMeasure, measureIntervals);
        
        for (Integer measure : query.getMeasures()) {
            if (timeSeriesSpans.containsKey(measure)) {
                List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
                cache.addToCache(spans);
                LOG.info("Added {} spans for measure {} with interval {}", 
                    spans.size(), measure, measureIntervals.get(measure));
            }
        }
        
        // For completeness, return the data
        Map<Integer, List<DataPoint>> resultData = new HashMap<>();
        for (Integer measure : query.getMeasures()) {
            if (!timeSeriesSpans.containsKey(measure)) continue;
            
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPointList = new ArrayList<>();
            
            for (TimeSeriesSpan span : spans) {
                if (span instanceof AggregateTimeSeriesSpan) {
                    Iterator<AggregatedDataPoint> it = ((AggregateTimeSeriesSpan) span).iterator();
                    while (it.hasNext()) {
                        AggregatedDataPoint point = it.next();
                        Stats stats = point.getStats();
                        
                        // Add key points (first, min, max, last)
                        dataPointList.add(new ImmutableDataPoint(stats.getFirstTimestamp(), stats.getFirstValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getMinTimestamp(), stats.getMinValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getMaxTimestamp(), stats.getMaxValue(), measure));
                        dataPointList.add(new ImmutableDataPoint(stats.getLastTimestamp(), stats.getLastValue(), measure));
                    }
                }
            }
            resultData.put(measure, dataPointList);
        }
        
        double queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        
        queryResults.setData(resultData);
        queryResults.setTimeRange(new TimeRange(from, to));
        queryResults.setQueryTime(queryTime);
        
        LOG.info("Completed measure-specific interval query execution in {} seconds", queryTime);
        return queryResults;
    }

    private VisualQueryResults executeM4Query(VisualQuery query) {
        VisualQueryResults queryResults = new VisualQueryResults();
        Map<Integer, List<DataPoint>> m4Data = new HashMap<>();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<TimeInterval>> missingΙntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, AggregateInterval> aggregateIntervals = new HashMap<>(query.getMeasures().size());

        long interval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
        AggregateInterval aggInterval = AggregateInterval.of(interval, ChronoUnit.MILLIS);
        long startPixelColumn = query.getFrom();
        long endPixelColumn = query.getFrom() + interval * (query.getViewPort().getWidth());

        for (Integer measure : query.getMeasures()) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getFrom() + interval * (query.getViewPort().getWidth())));
            missingΙntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            aggregateIntervals.put(measure, aggInterval);
        }

        AggregatedDataPoints missingDataPoints = 
            dataSource.getAggregatedDataPoints(startPixelColumn, endPixelColumn, missingΙntervalsPerMeasure, aggregateIntervals);
        
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingΙntervalsPerMeasure, aggregateIntervals);
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPoints = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                Iterator<AggregatedDataPoint> it = ((AggregateTimeSeriesSpan) span).iterator();
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

        return queryResults;
    }
}
