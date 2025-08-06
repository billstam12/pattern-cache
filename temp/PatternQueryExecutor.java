package gr.imsi.athenarc.middleware.pattern;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregationFactorService;
import gr.imsi.athenarc.middleware.cache.CacheUtils;
import gr.imsi.athenarc.middleware.cache.SlopeAggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.SlopeFunction;
import gr.imsi.athenarc.middleware.domain.Stats;
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
import gr.imsi.athenarc.middleware.sketch.PixelColumn;
import gr.imsi.athenarc.middleware.sketch.PixelSketch;
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.sketch.SketchUtils;
import gr.imsi.athenarc.middleware.visual.VisualEvaluator;

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
                case "visual":
                    sketch = new PixelColumn(sketchStart, sketchEnd);
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
        List<List<List<Sketch>>> matches = executePatternMatchingByMethod(sketches, patternNodes, params, method);
        
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
     * Execute pattern matching based on the method type
     */
    private static List<List<List<Sketch>>> executePatternMatchingByMethod(List<Sketch> sketches, 
                                                                          List<PatternNode> patternNodes, 
                                                                          QueryParams params, String method) {
        if (method.equals("visual")) {
            return executeVisualPatternMatching(sketches, patternNodes, params);
        } else {
            return executePatternMatching(sketches, patternNodes, false);
        }
    }

    /**
     * Handle visual pattern matching with multiple visualizations
     */
    private static List<List<List<Sketch>>> executeVisualPatternMatching(List<Sketch> sketches, 
                                                                        List<PatternNode> patternNodes, 
                                                                        QueryParams params) {
        LOG.info("Using visual method with {} pixel sketches", sketches.size());
        
        List<List<List<Sketch>>> allMatches = new ArrayList<>();
        Set<String> seenMatches = new HashSet<>(); // Track matches by their time ranges to avoid duplicates
        List<List<PixelColumn>> visualizations = getSeparateVisualizations(sketches, params.viewPort);

        for (int visualizationId = 0; visualizationId < visualizations.size(); visualizationId++) {
            List<PixelColumn> visualization = visualizations.get(visualizationId);
            
            // Process each visualization for pattern matching
            List<List<List<Sketch>>> visualizationMatches = processVisualizationForPatternMatching(
                    visualization, patternNodes, params, visualizationId);
            
            // Filter out duplicate matches based on time ranges
            int newMatchesCount = addUniqueMatches(visualizationMatches, allMatches, seenMatches);
            
            if (newMatchesCount > 0) {
                LOG.debug("Visualization {}: Found {} new unique pattern matches (filtered {} duplicates)", 
                        visualizationId, newMatchesCount, visualizationMatches.size() - newMatchesCount);
            }
        }
        
        LOG.info("Visual pattern matching completed, found {} total matches across {} visualizations", 
                allMatches.size(), visualizations.size());
        return allMatches;
    }

    /**
     * Process a single visualization for pattern matching
     */
    private static List<List<List<Sketch>>> processVisualizationForPatternMatching(List<PixelColumn> visualization, 
                                                                                  List<PatternNode> patternNodes, 
                                                                                  QueryParams params, 
                                                                                  int visualizationId) {
        // USE ERROR CALCULATOR TO COMPUTE THE ACTUAL PIXEL RANGES FOR EACH COLUMN
        VisualEvaluator visualEvaluator = new VisualEvaluator();
        double visualError = visualEvaluator.calculateTotalError(visualization, params.viewPort, params.timeUnit, params.accuracy);
        Stats viewPortStats = visualEvaluator.getViewPortStats();
        
        if (!visualEvaluator.hasError()) {
            // Convert PixelColumns to Sketches for pattern matching
            List<Sketch> visualizationSketches = convertPixelColumnsToSketches(visualization, params.viewPort, viewPortStats);
            
            // Apply NFA pattern matching to this specific visualization using the pixel-based sketches
            List<List<List<Sketch>>> visualizationMatches = executePatternMatching(visualizationSketches, patternNodes, false);
            
            LOG.debug("Processed visualization {} with {} pixel-based sketches", visualizationId, visualizationSketches.size());
            return visualizationMatches;
        } else {
            LOG.warn("Visualization {} has errors. Valid cols: {}, Vis error: {} skipping pattern matching", 
                    visualizationId, visualEvaluator.validColumns, visualError);
            return new ArrayList<>(); // Return empty list for visualizations with errors
        }
    }

    /**
     * Convert PixelColumns to PixelSketches for pattern matching
     */
    private static List<Sketch> convertPixelColumnsToSketches(List<PixelColumn> visualization, 
                                                             ViewPort viewPort, Stats viewPortStats) {
        List<Sketch> visualizationSketches = new ArrayList<>();
        for (PixelColumn pixelColumn : visualization) {
            if (pixelColumn.getPixelColumnRange() == null) {
                LOG.warn("PixelColumn {} has null pixelColumnRange, skipping", pixelColumn);
                continue;
            }
            PixelSketch pixelSketch = new PixelSketch(pixelColumn, viewPort, viewPortStats);
            visualizationSketches.add(pixelSketch);
        }
        return visualizationSketches;
    }

    /**
     * Add unique matches to the collection, filtering out duplicates
     */
    private static int addUniqueMatches(List<List<List<Sketch>>> visualizationMatches, 
                                       List<List<List<Sketch>>> allMatches, 
                                       Set<String> seenMatches) {
        int newMatchesCount = 0;
        for (List<List<Sketch>> match : visualizationMatches) {
            String matchSignature = createMatchSignature(match);
            if (!seenMatches.contains(matchSignature)) {
                seenMatches.add(matchSignature);
                allMatches.add(match);
                newMatchesCount++;
            }
        }
        return newMatchesCount;
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
        List<Sketch> sketches = prepareSketchesFromDataSource(params, dataSource, method);
        
        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = executePatternMatching(sketches, patternNodes, false);
        return createPatternQueryResults(matches, startTime);
    }


    /**
     * Prepare sketches by fetching data directly from data source (no caching)
     */
    private static List<Sketch> prepareSketchesFromDataSource(QueryParams params, DataSource dataSource, String method) {
        // Create timestamped sketches for direct data source pattern matching
        List<Sketch> sketches = generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, method);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);

        // Fetch data based on method type
        switch (method) {
            case "ols":
                executePatternQueryForSlopeMethod(
                    sketches, dataSource, params.measure,
                    params.alignedFrom, params.alignedTo, params.timeUnit);
                break;
            case "firstLastInf":
            case "firstLast":
                executePatternQueryForAggregateMethod(
                    sketches, dataSource, params.measure,
                    params.alignedFrom, params.alignedTo, params.timeUnit, method);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method for pattern query: " + method);
        }
        
        return sketches;
    }

     /**
     * Fetch all required data directly from data source (no caching).
     * Used by the non-cached pattern query execution.
    */
    private static void executePatternQueryForSlopeMethod(List<Sketch> sketches, DataSource dataSource, 
                                               int measure, long alignedFrom, long alignedTo, 
                                               AggregateInterval timeUnit) {
        // Create a time range for the pattern query (entire range)
        TimeRange patternTimeRange = new TimeRange(alignedFrom, alignedTo);
        
        // Setup measure and intervals
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        List<TimeInterval> intervals = new ArrayList<>();  
        intervals.add(patternTimeRange);
        intervalsPerMeasure.put(measure, intervals);
        
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        
        // Fetch all data directly from the data source
        AggregatedDataPoints newDataPoints = dataSource.getSlopeAggregates(
                alignedFrom, alignedTo, intervalsPerMeasure, aggregateIntervalsPerMeasure);
                        
        // Create spans and add to sketches
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
            TimeSeriesSpanFactory.createSlopeAggregate(newDataPoints, intervalsPerMeasure, aggregateIntervalsPerMeasure);
        
        // Fill the sketches with the data
        for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
            for (TimeSeriesSpan span : spans) {
                if(span instanceof SlopeAggregateTimeSeriesSpan) {
                    SlopeAggregateTimeSeriesSpan aggregateSpan = (SlopeAggregateTimeSeriesSpan) span;
                    Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                    while (dataPoints.hasNext()) {
                        AggregatedDataPoint point = dataPoints.next();
                        SketchUtils.addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                    }
                } else {
                   throw new IllegalArgumentException("Unsupported span type for M4 patterns: " + span.getClass());     
                }
            }
        }
    }

    /**
     * Fetch all required data directly from data source (no caching).
     * Used by the non-cached pattern query execution.
    */ 
    private static void executePatternQueryForAggregateMethod(List<Sketch> sketches, DataSource dataSource,
            int measure, long from, long to, AggregateInterval timeUnit, String method) {
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        List<TimeInterval> intervals = new ArrayList<>();
        intervals.add(new TimeRange(from, to));
        intervalsPerMeasure.put(measure, intervals);
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        Set<String> aggregateFunctions = AggregationFunctionsConfig.getAggregateFunctions(method);
        AggregatedDataPoints dataPoints;
        switch (method) {
            case "firstLastInf":
                dataPoints = dataSource.getAggregatedDataPoints(
                    from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
                break;
            case "firstLast":
                dataPoints = dataSource.getAggregatedDataPointsWithTimestamps(
                    from, to, intervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method for single aggregate pattern query: " + method);
        }
        SketchUtils.populateSketchesFromDataPoints(dataPoints.iterator(), sketches,
            from, to, timeUnit);
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
            case "visual":
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
            || method.equalsIgnoreCase("visual")
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
     * Log pattern matches to a file for later comparison
     */
    public static void logMatchesToFile(PatternQuery query, PatternQueryResults patternQueryResults, String method, DataSource dataSource, String outputFolder) {

        QueryParams params = extractQueryParams(query);
        List<List<List<Sketch>>> matches = patternQueryResults.getMatches();
        long executionTime = patternQueryResults.getExecutionTime();

        try {
            
            // Create matches directory if it doesn't exist
            Path patternMatchesDir = Paths.get(outputFolder,"pattern_matches");
            if (!Files.exists(patternMatchesDir)) {
                Files.createDirectories(patternMatchesDir);
            }
          
            Path methodDir = Paths.get(outputFolder, "pattern_matches", method);
            if (!Files.exists(methodDir)) {
                Files.createDirectories(methodDir);
            }

            Path dbDir = Paths.get(outputFolder, "pattern_matches", method, "influx");
            if (!Files.exists(dbDir)) {
                Files.createDirectories(dbDir);
            }

            // Create specific directory if it doesn't exist
            Path logDir = Paths.get(outputFolder,"pattern_matches", method, "influx", dataSource.getDataset().getTableName());
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
            }
            
            // Create a unique filename with timestamp
            String filename = String.format("%s_%s_%d_%s.log", 
                params.from, params.to, params.measure, params.timeUnit.toString());
            
            File logFile = new File(logDir.toFile(), filename);
            
            try (FileWriter writer = new FileWriter(logFile)) {
                // Write metadata
                writer.write("Query Metadata:\n");
                writer.write(String.format("Time Range: %d to %d\n", params.from, params.to));
                writer.write(String.format("Aligned Time Range: %d to %d\n", params.alignedFrom, params.alignedTo));
                writer.write(String.format("Measure ID: %d\n", params.measure));
                writer.write(String.format("Time Unit: %s\n", params.timeUnit));
                writer.write(String.format("Execution Time: %d ms\n", executionTime));
                writer.write(String.format("Total Matches: %d\n", matches.size()));
                writer.write("\n--- Matches ---\n\n");
                
                // Write each match with simplified information
                for (int matchIdx = 0; matchIdx < matches.size(); matchIdx++) {
                    List<List<Sketch>> match = matches.get(matchIdx);
                    
                    // Calculate the overall match time range and error margin
                    long matchStart = Long.MAX_VALUE;
                    long matchEnd = Long.MIN_VALUE;
                    double totalErrorMargin = 0.0;
                    int segmentCount = 0;
                    
                    for (List<Sketch> segment : match) {
                        Sketch combinedSketch = SketchUtils.combineSketches(segment);
                        matchStart = Math.min(matchStart, combinedSketch.getFrom());
                        matchEnd = Math.max(matchEnd, combinedSketch.getTo());
                        
                        // Accumulate error margins for average calculation
                        double segmentError = combinedSketch.getAngleErrorMargin();
                        if (!Double.isNaN(segmentError) && !Double.isInfinite(segmentError)) {
                            totalErrorMargin += segmentError;
                            segmentCount++;
                        }
                    }
                    
                    // Calculate average error margin for the match
                    double averageErrorMargin = segmentCount > 0 ? totalErrorMargin / segmentCount : 0.0;

                    writer.write(String.format("Match #%d: [%d to %d] - Average Error Margin: %.2f%%\n", 
                        matchIdx + 1, matchStart, matchEnd, averageErrorMargin * 100.0));

                    for (int segmentIdx = 0; segmentIdx < match.size(); segmentIdx++) {
                        List<Sketch> segment = match.get(segmentIdx);
                        Sketch combinedSketch = SketchUtils.combineSketches(segment);
                        double segmentErrorMargin = combinedSketch.getAngleErrorMargin();
                        writer.write(String.format("  Segment %d: [%d to %d] - Error Margin: %.2f%%\n", 
                            segmentIdx, combinedSketch.getFrom(), combinedSketch.getTo(), segmentErrorMargin * 100.0));
                    }
                    writer.write("\n");
                }
            }
            
            LOG.info("Pattern matches logged to file: {}", logFile.getAbsolutePath());
            
        } catch (IOException e) {
            LOG.error("Failed to log pattern matches to file", e);
        }
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
    
    /**
     * Creates a unique signature for a pattern match based on the time ranges of its segments.
     * This is used to identify and filter out duplicate matches across visualizations.
     * 
     * @param match The pattern match consisting of segments (lists of sketches)
     * @return A string signature uniquely identifying this match
     */
    private static String createMatchSignature(List<List<Sketch>> match) {
        StringBuilder signature = new StringBuilder();
        
        for (int segmentIdx = 0; segmentIdx < match.size(); segmentIdx++) {
            List<Sketch> segment = match.get(segmentIdx);
            if (!segment.isEmpty()) {
                // Get the time range of this segment
                long segmentStart = segment.get(0).getFrom();
                long segmentEnd = segment.get(segment.size() - 1).getTo();
                
                if (segmentIdx > 0) {
                    signature.append("|");
                }
                signature.append(segmentStart).append("-").append(segmentEnd);
            }
        }
        
        return signature.toString();
    }
    
    public static List<List<PixelColumn>> getAllVisualizations(List<Sketch> sketches, ViewPort viewPort) {
        List<List<PixelColumn>> visualizations = new ArrayList<>();
        int startIndex = 0;

        int noOfVisualizations = sketches.size() - viewPort.getWidth() + 1;

        while(startIndex < noOfVisualizations){
            // Create visualization with pixel columns based on viewport width
            List<PixelColumn> visualization = new ArrayList<>();
            for(int i = startIndex; i < startIndex + viewPort.getWidth(); i++){
                Sketch sketch = sketches.get(i);
                if(sketch instanceof PixelColumn){
                    PixelColumn pixelColumn = (PixelColumn) sketch;
                    visualization.add(pixelColumn.clone());
                } else {
                    throw new IllegalArgumentException("Expected PixelColumn for visual method, got: " + sketch.getClass());
                }  
            }
            visualizations.add(visualization);
            startIndex++;
        }
        return visualizations;
    }

    public static List<List<PixelColumn>> getSeparateVisualizations(List<Sketch> sketches, ViewPort viewPort) {
        List<List<PixelColumn>> visualizations = new ArrayList<>();
        int startIndex = 0;
        int viewportWidth = viewPort.getWidth();
        
        // Create full non-overlapping visualizations
        while(startIndex + viewportWidth <= sketches.size()){
            // Create visualization with pixel columns based on viewport width
            List<PixelColumn> visualization = new ArrayList<>();
            for(int i = startIndex; i < startIndex + viewportWidth; i++){
                Sketch sketch = sketches.get(i);
                if(sketch instanceof PixelColumn){
                    PixelColumn pixelColumn = (PixelColumn) sketch;
                    visualization.add(pixelColumn.clone());
                } else {
                    throw new IllegalArgumentException("Expected PixelColumn for visual method, got: " + sketch.getClass());
                }  
            }
            visualizations.add(visualization);
            startIndex += viewportWidth;
        }
        
        // Handle remaining sketches by creating a final visualization with padding from previous
        if(startIndex < sketches.size()) {
            int remainingSketches = sketches.size() - startIndex;
            int paddingNeeded = viewportWidth - remainingSketches;
            
            // Calculate new start index to include padding from previous sketches
            int finalStartIndex = startIndex - paddingNeeded;
            
            List<PixelColumn> visualization = new ArrayList<>();
            for(int i = finalStartIndex; i < sketches.size(); i++){
                Sketch sketch = sketches.get(i);
                if(sketch instanceof PixelColumn){
                    PixelColumn pixelColumn = (PixelColumn) sketch;
                    visualization.add(pixelColumn.clone());
                } else {
                    throw new IllegalArgumentException("Expected PixelColumn for visual method, got: " + sketch.getClass());
                }  
            }
            visualizations.add(visualization);
        }
        
        return visualizations;
    }
}
