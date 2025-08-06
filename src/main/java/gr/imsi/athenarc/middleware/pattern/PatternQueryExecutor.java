package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregationFactorService;
import gr.imsi.athenarc.middleware.cache.CacheUtils;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.SlopeFunction;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.sketch.ApproxOLSSketch;
import gr.imsi.athenarc.middleware.sketch.FirstLastSketch;
import gr.imsi.athenarc.middleware.sketch.ApproxFirstLastSketch;
import gr.imsi.athenarc.middleware.sketch.OLSSketch;
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.sketch.SketchUtils;

public class PatternQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryExecutor.class);
    
    /**
     * Generate sketches covering the specified time range based on the AggregateInterval.
     * 
     * @param from Start timestamp (already aligned to time unit boundary)
     * @param to End timestamp (already aligned to time unit boundary)
     * @param timeUnit Aggregate interval for sketches
     * @param aggregationType Type of aggregation for the sketches
     * @return List of sketches spanning the time range
     */
    public static List<Sketch> generateSketches(long from, long to, AggregateInterval timeUnit, String method) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Calculate the number of complete intervals
        long unitDurationMs = timeUnit.toDuration().toMillis();
        int numIntervals = DateTimeUtil.numberOfIntervals(from, to, timeUnit);
        
        // Create a sketch for each interval
        for (int i = 0; i < numIntervals; i++) {
            long sketchStart = from + (i * unitDurationMs);
            long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
            Sketch sketch = null;
            switch(method){
                case "firstLast":
                case "firstLastInf":
                case "m4":
                case "m4Inf":
                    sketch = new FirstLastSketch(sketchStart, sketchEnd);
                    break;
                case "minmax":
                    sketch = new ApproxFirstLastSketch(sketchStart, sketchEnd);
                    break;
                case "approxOls":
                    sketch = new ApproxOLSSketch(sketchStart, sketchEnd, i);
                    break;
                case "ols":
                    sketch = new OLSSketch(sketchStart, sketchEnd);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown method: " + method);
            }
            sketches.add(sketch);
        }
        
        return sketches;
    }


    /**
     * Execute a pattern query with caching support.
     * This method handles cache lookups and updates for pattern matching.
     * 
     * @param query The pattern query to execute
     * @param dataSource The data source to use for fetching missing data
     * @param cache The cache to check for existing data and to update with new data
     * @param method The method type (e.g., "m4Inf", "m4", "minmax", etc.)
     * @return Pattern query results
     */
    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource, 
                                                                  TimeSeriesCache cache, String method) {        
        long startTime = System.currentTimeMillis();

        // Extract query parameters and prepare sketches
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        List<Sketch> sketches = prepareSketchesWithCache(params, dataSource, cache, method);
        
        // Execute pattern matching based on method type
        List<List<List<Sketch>>> matches = executePatternMatching(sketches, patternNodes, false);
        
        // Return results
        return createPatternQueryResults(matches, startTime);
    }

    /**
     * Prepare sketches with cache support - handles sketch creation, cache population, and missing data fetching
     */
    private static List<Sketch> prepareSketchesWithCache(QueryParams params, DataSource dataSource, 
                                                        TimeSeriesCache cache, String method) {
        // Create sketches for non-timestamped pattern matching (used with cache)
        List<Sketch> sketches = generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, method);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);
        
        // Check cache and populate sketches with existing data
        populateSketchesFromCache(
                sketches, cache, method, params.measure, 
                params.alignedFrom, params.alignedTo, params.timeUnit, params.viewPort);
        
        // Fetch missing data from datasource and update cache
        fetchMissingDataAndUpdateCache(
                sketches, dataSource, cache, method,
                params.measure, params.alignedFrom, params.alignedTo,
                params.timeUnit, params.viewPort);
        
        return sketches;
    }

    /**
     * Create pattern query results with execution time
     */
    private static PatternQueryResults createPatternQueryResults(List<List<List<Sketch>>> matches, long startTime) {
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        LOG.info("Pattern query executed in {} ms", executionTime);
        
        PatternQueryResults patternQueryResults = new PatternQueryResults();
        patternQueryResults.setMatches(matches);
        patternQueryResults.setExecutionTime(executionTime);
        return patternQueryResults;
    }
    
    /**
     * Execute a pattern query directly without using cache.
     * This method fetches all required data directly from the data source.
     * 
     * @param query The pattern query to execute
     * @param dataSource The data source to use for fetching data
     * @param method The method type (e.g., "m4", "minmax", "ols", etc.)
     * @return Pattern query results
     */
    public static PatternQueryResults executePatternQuery(PatternQuery query, DataSource dataSource, String method) {
        long startTime = System.currentTimeMillis();

        // Handle MATCH_RECOGNIZE queries separately
        if ("matchRecognize".equals(method)) {
            return MatchRecognizeQueryExecutor.executeMatchRecognizeQuery(query, dataSource);
        }

        // Extract query parameters and prepare sketches
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();

         // Create timestamped sketches for direct data source pattern matching
        List<Sketch> sketches = prepareAndPopulateSketches(dataSource, method, params.measure,
                params.alignedFrom, params.alignedTo, params.timeUnit);
        
        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = executePatternMatching(sketches, patternNodes, false);
        return createPatternQueryResults(matches, startTime);
    }


    /**
     * Prepare sketches by fetching data directly from data source (no caching)
     */
    private static List<Sketch> prepareAndPopulateSketches(DataSource dataSource, String method,
                                                             int measure, long from, long to,
                                                             AggregateInterval timeUnit) {
        List<Sketch> sketches = generateSketches(from, to, timeUnit, method);
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        List<TimeInterval> intervals = new ArrayList<>();
        intervals.add(new TimeRange(from, to));
        intervalsPerMeasure.put(measure, intervals);
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        AggregatedDataPoints dataPoints;
        switch (method) {
            case "firstLastInf":
                dataPoints = dataSource.getAggregatedDataPoints(
                    from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure, AggregationFunctionsConfig.getAggregateFunctions(method));
                break;
            case "firstLast":
                dataPoints = dataSource.getAggregatedDataPointsWithTimestamps(
                    from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure, AggregationFunctionsConfig.getAggregateFunctions(method));
                break;
            case "ols":
                dataPoints = dataSource.getSlopeAggregates(
                        from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method for pattern query: " + method);
        }
        SketchUtils.populateSketchesFromDataPoints(dataPoints.iterator(), sketches, from, to, timeUnit);
        return sketches;
    }

    /**
     * Helper class to store query parameters extracted from a PatternQuery
     */
    private static class QueryParams {
        final long from;
        final long to;
        final long alignedFrom;
        final long alignedTo;
        final double accuracy;
        final int measure;
        final AggregateInterval timeUnit;
        final ViewPort viewPort;
        
        QueryParams(long from, long to, long alignedFrom, long alignedTo, double accuracy,
                   int measure, AggregateInterval timeUnit, ViewPort viewPort, SlopeFunction slopeFunction) {
            this.from = from;
            this.to = to;
            this.alignedFrom = alignedFrom;
            this.alignedTo = alignedTo;
            this.accuracy = accuracy;
            this.measure = measure;
            this.timeUnit = timeUnit;
            this.viewPort = viewPort;
        }
    }
    
    /**
     * Extract common parameters from a pattern query.
     * Also handles time alignment based on the query's time unit.
     */
    private static QueryParams extractQueryParams(PatternQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasures().get(0); // for now pattern querys have only one measure
        AggregateInterval timeUnit = query.getTimeUnit();
        SlopeFunction slopeFunction = query.getSlopeFunction();
        // Align start and end times to the time unit boundaries for proper alignment
        long alignedFrom = DateTimeUtil.alignToTimeUnitBoundary(from, timeUnit, true);  // floor
        long alignedTo = DateTimeUtil.alignToTimeUnitBoundary(to, timeUnit, false);     // ceiling
        double accuracy =  query.getAccuracy();
        ViewPort viewPort = query.getViewPort();
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        
        return new QueryParams(from, to, alignedFrom, alignedTo, accuracy, measure, timeUnit, viewPort, slopeFunction);
    }
    
    /**
     * Populate sketches with data from cache if available.
     */
    private static void populateSketchesFromCache(List<Sketch> sketches, TimeSeriesCache cache, String method,
                                              int measure, long alignedFrom, long alignedTo, 
                                              AggregateInterval timeUnit, ViewPort viewPort) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        
        TimeRange alignedTimeRange = new TimeRange(alignedFrom, alignedTo);

        List<TimeSeriesSpan> existingSpans;
        switch(method){
            case "firstLast":
            case "firstLastInf":
            case "m4":
            case "m4Inf":
                existingSpans = cache.getCompatibleSpans(measure, alignedTimeRange, timeUnit);
                break;
            case "minmax":
            case "approxOls":
                existingSpans = cache.getOverlappingSpansForVisualization(measure, alignedTimeRange, timeUnit);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method for pattern query: " + method);
        }

        if (!existingSpans.isEmpty()) {
            LOG.info("Found {} existing compatible spans in cache for measure {}", existingSpans.size(), measure);
            SketchUtils.populateSketchesFromSpans(existingSpans, sketches, alignedFrom, alignedTo, timeUnit, viewPort, method);
        }
    }

    /**
     * Fetch missing data from data source and update cache.
     */
    private static void fetchMissingDataAndUpdateCache(List<Sketch> sketches, DataSource dataSource, 
                                                   TimeSeriesCache cache,  String method, int measure, 
                                                   long alignedFrom, long alignedTo, 
                                                   AggregateInterval timeUnit, 
                                                   ViewPort viewPort
                                                ) {
                                            
        // Identify unfilled sketches/intervals
        //round down        
        List<TimeInterval> missingIntervals = identifyMissingIntervals(sketches, alignedFrom, alignedTo);
        AggregateInterval subInterval;
        AggregationFactorService aggFactorService = AggregationFactorService.getInstance();

        int divider = 1;

        if(method.equalsIgnoreCase("minmax")
            || method.equalsIgnoreCase("approxOls")) {
            divider = aggFactorService.getAggFactor(measure);
        } 

        subInterval = DateTimeUtil.roundDownToCalendarBasedInterval(timeUnit.toDuration().toMillis() / divider);

        if (!missingIntervals.isEmpty()) {
            // For better performance, merge adjacent intervals while preserving alignment
            List<TimeInterval> mergedMissingIntervals = DateTimeUtil.groupIntervals(timeUnit, missingIntervals);
            LOG.info("Merged into {} intervals for fetching", mergedMissingIntervals.size());
            
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
            missingIntervalsPerMeasure.put(measure, mergedMissingIntervals);
            
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();            
            aggregateIntervalsPerMeasure.put(measure, subInterval);
            
            Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure = 
                DateTimeUtil.alignIntervalsToTimeUnitBoundary(missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
            
            Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
                CacheUtils.fetchTimeSeriesSpans(dataSource, alignedFrom, alignedTo, 
                                     alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, method);

            for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
                if (cache != null) {
                    cache.addToCache(spans);
                } else {
                    throw new IllegalArgumentException("Cache is null, cannot add spans");
                }
                SketchUtils.populateSketchesFromSpans(spans, sketches, alignedFrom, alignedTo, timeUnit, viewPort, method);
            }
        } else {
            LOG.info("All required data available in cache, no need for additional fetching");
        }
    }
    

    /**
     * Perform pattern matching on the prepared sketches and generate results.
     * @param allowOverlapping if true, finds all possible matches including overlapping ones.
     *                        if false, finds non-overlapping matches using a greedy approach.
     */
    private static List<List<List<Sketch>>> executePatternMatching(List<Sketch> sketches, 
                                                         List<PatternNode> patternNodes, 
                                                         boolean allowOverlapping) {
        LOG.info("Starting search, over {} aggregate data.", sketches.size());
        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes);
        
        long startTime = System.currentTimeMillis();
        List<List<List<Sketch>>> matches = sketchSearch.findAllMatches(allowOverlapping);
        long endTime = System.currentTimeMillis();

        LOG.info("Pattern matching completed in {} ms, found {} matches (overlapping={})", 
                (endTime - startTime), matches.size(), allowOverlapping);
        return matches;
    }
    
    
    /**
     * Identifies missing intervals in the sketches that need to be fetched from the data source.
     * Ensures intervals are aligned with time unit boundaries.
     * If a sketch has aggregated data points with count=0, we know that interval has no data
     * in the underlying database, so we don't mark it as missing.
     * 
     * @param sketches The sketches representing the full time range
     * @param from Start time (already aligned)
     * @param to End time (already aligned)
     * @param timeUnit The time unit for aggregation
     * @return List of missing intervals that need to be fetched
     */
    public static List<TimeInterval> identifyMissingIntervals(List<Sketch> sketches, long from, long to) {
        List<TimeInterval> missingIntervals = new ArrayList<>();
        
        for (int i = 0; i < sketches.size(); i++) {
            Sketch sketch = sketches.get(i);
            
            // If sketch has not initialized, it means we dont have its data in the cache
            if (!sketch.hasInitialized()) {
                missingIntervals.add(sketch);
            }
        }
        LOG.info("Identified {} missing intervals", missingIntervals.size());
        return missingIntervals;
    }
}
