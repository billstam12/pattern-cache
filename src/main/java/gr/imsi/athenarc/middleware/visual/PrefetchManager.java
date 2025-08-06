package gr.imsi.athenarc.middleware.visual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.*;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrefetchManager {

    private final AbstractDataset dataset;
    private final DataProcessor dataProcessor;
    private final TimeSeriesCache cache;
    private final double prefetchingFactor;

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchManager.class);

    protected PrefetchManager(DataSource dataSource, double prefetchingFactor,
                           TimeSeriesCache cache, DataProcessor dataProcessor) {
        this.prefetchingFactor = prefetchingFactor;
        this.cache = cache;
        this.dataProcessor = dataProcessor;
        this.dataset = dataSource.getDataset();
    }

    private long[] extendInterval(long from, long to, double factor){
        long interval = to - from;
        long difference = (long) (interval * (factor / 2));
        long newFrom = Math.max(dataset.getTimeRange().getFrom(), from - difference);
        long newTo = Math.min(dataset.getTimeRange().getTo(), to + difference);

        return new long[]{newFrom, newTo};
    }

    protected void prefetch(VisualQuery query, Map<Integer, Integer> aggFactors){
        if(prefetchingFactor == 0) return;
        // Setup prefetching range
        long[] prefetchingInterval = extendInterval(query.getFrom(), query.getTo(), prefetchingFactor);
        long prefetchingFrom = prefetchingInterval[0];
        long prefetchingTo = prefetchingInterval[1];

        // Initialize prefetch query
        // Then fetch data similarly to CacheQueryExecutor
        VisualQuery prefetchQuery = new VisualQuery(prefetchingFrom, prefetchingTo, query.getMeasures(), query.getViewPort().getWidth(), query.getViewPort().getHeight(),  query.getAccuracy());
        List<Integer> measures = prefetchQuery.getMeasures();
        ViewPort viewPort = prefetchQuery.getViewPort();
        long from = prefetchQuery.getFrom();
        long to = prefetchQuery.getTo();
        long pixelColumnIntervalMillis = (to - from) / viewPort.getWidth();
        AggregateInterval pixelColumnInterval = DateTimeUtil.roundDownToCalendarBasedInterval(pixelColumnIntervalMillis);
        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure = cache.getFromCacheForVisualization(prefetchQuery, pixelColumnInterval);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(measures.size());
        // Initialize Pixel Columns
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = new HashMap<>(measures.size()); // Lists of pixel columns. One list for every measure.
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnInterval.toDuration().toMillis());
                long pixelTo = pixelFrom + pixelColumnInterval.toDuration().toMillis();
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo);
                pixelColumns.add(pixelColumn);
            }
            pixelColumnsPerMeasure.put(measure, pixelColumns);
        }

        for(int measure : measures) {
            // Get overlapping spans
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(measure);

            // Add to pixel columns
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);

            // Calculate Error
            VisualEvaluator errorCalculator = new VisualEvaluator();
            errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, prefetchQuery.getAccuracy());
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();
            if(!missingIntervalsForMeasure.isEmpty())
                missingIntervalsPerMeasure.put(measure, missingIntervalsForMeasure);
        }
        LOG.info("Prefetching: {}", missingIntervalsPerMeasure);
        if(missingIntervalsPerMeasure.isEmpty()) return;
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpanPerMeasure =
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactors, viewPort);
        // Add to cache directly
        for(int measureWithMiss : missingTimeSeriesSpanPerMeasure.keySet()) {
            cache.addToCache(missingTimeSeriesSpanPerMeasure.get(measureWithMiss));
        }
        LOG.info("Inserted new time series spans into interval tree");
    }
}
