package gr.imsi.athenarc.middleware.pattern;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.client.domain.Query;

import gr.imsi.athenarc.middleware.cache.M4InfAggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.MinMaxAggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.RawTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.CacheUtils;
import gr.imsi.athenarc.middleware.cache.M4AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.SlopeAggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.sketch.ApproxOLSSketch;
import gr.imsi.athenarc.middleware.sketch.M4Sketch;
import gr.imsi.athenarc.middleware.sketch.MinMaxSketch;
import gr.imsi.athenarc.middleware.sketch.OLSSketch;
import gr.imsi.athenarc.middleware.sketch.Sketch;

public class PatternUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PatternUtils.class);
    
    /**
     * Generate sketches covering the specified time range based on the AggregateInterval.
     * 
     * @param from Start timestamp (already aligned to time unit boundary)
     * @param to End timestamp (already aligned to time unit boundary)
     * @param timeUnit Aggregate interval for sketches
     * @param aggregationType Type of aggregation for the sketches
     * @return List of sketches spanning the time range
     */
    public static List<Sketch> generateSketches(long from, long to, AggregateInterval timeUnit, AggregationType aggregationType, ViewPort viewPort, String type) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Calculate the number of complete intervals
        long unitDurationMs = timeUnit.toDuration().toMillis();
        int numIntervals = DateTimeUtil.numberOfIntervals(from, to, timeUnit);
        
        // Create a sketch for each interval
        for (int i = 0; i < numIntervals; i++) {
            long sketchStart = from + (i * unitDurationMs);
            long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
            Sketch sketch = null;
            switch(type){
                case "m4":
                    sketch = new M4Sketch(sketchStart, sketchEnd, aggregationType);
                    break;
                case "m4Inf":
                    sketch = new M4Sketch(sketchStart, sketchEnd, aggregationType);
                    break;
                case "minmax":
                    sketch = new MinMaxSketch(sketchStart, sketchEnd, aggregationType);
                    break;
                case "ols":
                    sketch = new OLSSketch(sketchStart, sketchEnd, aggregationType);
                    break;
                case "approxols":
                    sketch = new ApproxOLSSketch(sketchStart, sketchEnd, i);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown sketch type: " + type);
            }
            sketches.add(sketch);
        }
        
        return sketches;
    }

    public static Sketch combineSketches(List<Sketch> sketchs){
        if(sketchs == null || sketchs.isEmpty()){
            throw new IllegalArgumentException("Cannot combine empty list of sketches");
        }
        Sketch firstSketch = sketchs.get(0);
        Sketch combinedSketch = firstSketch.clone();

        for(int i = 1; i < sketchs.size(); i++){
            combinedSketch.combine(sketchs.get(i));;
        }
        return combinedSketch;
    }

    /** 
     * Populate sketches with data from a TimeSeriesSpan.
     */
    private static void populateSketchesFromSpans(List<TimeSeriesSpan> spans, 
                                      List<Sketch> sketches, 
                                      long from, 
                                      long to, 
                                      AggregateInterval timeUnit, 
                                      ViewPort viewPort) {
        for(TimeSeriesSpan span : spans) {
            if (span instanceof RawTimeSeriesSpan) {
                    // Skip raw spans as they're not useful for pattern detection
                    continue;
            }
                    
            Iterator<AggregatedDataPoint> dataPoints = null;
            
            // Extract the appropriate iterator based on span type
            if (span instanceof M4InfAggregateTimeSeriesSpan) {
                dataPoints = ((M4InfAggregateTimeSeriesSpan) span).iterator(from, to);
            } else if (span instanceof M4AggregateTimeSeriesSpan) {
                dataPoints = ((M4AggregateTimeSeriesSpan) span).iterator(from, to);
            } else if (span instanceof MinMaxAggregateTimeSeriesSpan) {
                dataPoints = ((MinMaxAggregateTimeSeriesSpan) span).iterator(from, to);
            } else {
                throw new IllegalArgumentException("Unsupported span type for cached patterns: " + span.getClass());
            }
            
            // Process all data points from the span
            while (dataPoints != null && dataPoints.hasNext()) {
                AggregatedDataPoint point = dataPoints.next();
                addAggregatedDataPointToSketches(from, to, timeUnit, sketches, point, viewPort);
            }
        }
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

        Set<String> aggregateFunctions = AggregationFunctionsConfig.getAggregateFunctions("method");

        // Extract query parameters
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();

        // Create sketches for non-timestamped pattern matching (used with cache)
        List<Sketch> sketches = generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, 
                params.aggregationType, params.viewPort, method);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);
        
        // Check cache and populate sketches with existing data
        populateSketchesFromCache(
                sketches, cache, params.measure, 
                params.alignedFrom, params.alignedTo, params.timeUnit,  params.viewPort);
        
        // Fetch missing data from datasource and update cache
        fetchMissingDataAndUpdateCache(
                sketches, dataSource, cache, method,
                params.measure, params.alignedFrom, params.alignedTo, 
                params.timeUnit,  aggregateFunctions, params.viewPort);
        
        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = performPatternMatching(sketches, patternNodes);
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        LOG.info("Pattern query with cache executed in {} ms", executionTime);
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
    public static PatternQueryResults executePatternQuery(PatternQuery query, DataSource dataSource, String method){
        long startTime = System.currentTimeMillis();

        // Extract query parameters
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
                
        // Create timestamped sketches for direct data source pattern matching
        List<Sketch> sketches = generateSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, 
                params.aggregationType, params.viewPort, method);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);

        if(method.equals("ols")){
            fetchSlopeDataFromDataSource(
                sketches, dataSource, params.measure,
                params.alignedFrom, params.alignedTo, params.timeUnit, params.viewPort);
        }
        else {
            fetchMissingDataFromDataSource(
                sketches, dataSource, params.measure,
                params.alignedFrom, params.alignedTo, params.timeUnit, params.viewPort, method);
        }
        
        
        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = performPatternMatching(sketches, patternNodes);
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        LOG.info("Pattern query executed in {} ms", executionTime);
        
        PatternQueryResults patternQueryResults = new PatternQueryResults();
        patternQueryResults.setMatches(matches);
        patternQueryResults.setExecutionTime(executionTime);
        return patternQueryResults;
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
        final AggregationType aggregationType;
        final ViewPort viewPort;
        
        QueryParams(long from, long to, long alignedFrom, long alignedTo, double accuracy,
                   int measure, AggregateInterval timeUnit, ViewPort viewPort, AggregationType aggregationType) {
            this.from = from;
            this.to = to;
            this.alignedFrom = alignedFrom;
            this.alignedTo = alignedTo;
            this.accuracy = accuracy;
            this.measure = measure;
            this.timeUnit = timeUnit;
            this.viewPort = viewPort;
            this.aggregationType = aggregationType;
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
        AggregationType aggregationType = query.getAggregationType();
        
        // Align start and end times to the time unit boundaries for proper alignment
        long alignedFrom = DateTimeUtil.alignToTimeUnitBoundary(from, timeUnit, true);  // floor
        long alignedTo = DateTimeUtil.alignToTimeUnitBoundary(to, timeUnit, false);     // ceiling
        double accuracy =  query.getAccuracy();
        ViewPort viewPort = query.getViewPort();
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        
        return new QueryParams(from, to, alignedFrom, alignedTo, accuracy, measure, timeUnit, viewPort, aggregationType);
    }
    
    /**
     * Populate sketches with data from cache if available.
     */
    private static void populateSketchesFromCache(List<Sketch> sketches, TimeSeriesCache cache, 
                                              int measure, long alignedFrom, long alignedTo, 
                                              AggregateInterval timeUnit, ViewPort viewPort) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        
        TimeRange alignedTimeRange = new TimeRange(alignedFrom, alignedTo);

        List<TimeSeriesSpan> existingSpans = cache.getCompatibleSpans(measure, alignedTimeRange, timeUnit);

        if (!existingSpans.isEmpty()) {
            LOG.info("Found {} existing compatible spans in cache for measure {}", existingSpans.size(), measure);
            
            // Fill sketches with data from cache
            populateSketchesFromSpans(existingSpans, sketches, alignedFrom, alignedTo, timeUnit, viewPort);
        }
    }

    /**
     * Fetch missing data from data source and update cache.
     */
    private static void fetchMissingDataAndUpdateCache(List<Sketch> sketches, DataSource dataSource, 
                                                   TimeSeriesCache cache,  String method, int measure, 
                                                   long alignedFrom, long alignedTo, 
                                                   AggregateInterval timeUnit, 
                                                   Set<String> aggregateFunctions,
                                                   ViewPort viewPort
                                                ) {
        // Identify unfilled sketches/intervals
        //round down        
        List<TimeInterval> missingIntervals = identifyMissingIntervals(sketches, alignedFrom, alignedTo);
        
        AggregateInterval subInterval = DateTimeUtil.roundDownToCalendarBasedInterval(timeUnit.toDuration().toMillis());

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
                // Fill the sketches with the new data
                populateSketchesFromSpans(spans, sketches, alignedFrom, alignedTo, timeUnit, viewPort);
            }
        } else {
            LOG.info("All required data available in cache, no need for additional fetching");
        }
    }
    
    /**
     * Fetch all required data directly from data source (no caching).
     * Used by the non-cached pattern query execution.
     */
    private static void fetchMissingDataFromDataSource(List<Sketch> sketches, DataSource dataSource, 
                                               int measure, long alignedFrom, long alignedTo, 
                                               AggregateInterval timeUnit, ViewPort viewPort, String method) {
        // Create a time range for the pattern query (entire range)
        TimeRange patternTimeRange = new TimeRange(alignedFrom, alignedTo);
        
        // Setup measure and intervals
        Map<Integer, List<TimeInterval>> intervalsPerMeasure = new HashMap<>();
        List<TimeInterval> intervals = new ArrayList<>();  
        intervals.add(patternTimeRange);
        intervalsPerMeasure.put(measure, intervals);
        
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, timeUnit);
        
        // Create spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans =  
                CacheUtils.fetchTimeSeriesSpans(dataSource, alignedFrom, alignedTo, 
                                     intervalsPerMeasure, aggregateIntervalsPerMeasure, method);

        // Fill the sketches with the data
        for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
            populateSketchesFromSpans(spans, sketches, alignedFrom, alignedTo, timeUnit, viewPort);
        }
    }

    private static void fetchSlopeDataFromDataSource(List<Sketch> sketches, DataSource dataSource, 
                                               int measure, long alignedFrom, long alignedTo, 
                                               AggregateInterval timeUnit, ViewPort viewPort) {
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
                        addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point, viewPort);
                    }
                } else {
                   throw new IllegalArgumentException("Unsupported span type for M4 patterns: " + span.getClass());     
                }
            }
        }
    }
    
    /**
     * Perform pattern matching on the prepared sketches and generate results.
     */
    private static List<List<List<Sketch>>> performPatternMatching(List<Sketch> sketches, 
                                                         List<PatternNode> patternNodes) {
        LOG.info("Starting search, over {} aggregate data.", sketches.size());
        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes);
        
        long startTime = System.currentTimeMillis();
        List<List<List<Sketch>>> matches = sketchSearch.findAllMatches();
        long endTime = System.currentTimeMillis();

        LOG.info("Pattern matching completed in {} ms, found {} matches", 
                (endTime - startTime), matches.size());
        return matches;
    }
    
    
    /**
     * Log pattern matches to a file for later comparison
     */
    public static void logMatchesToFile(PatternQuery query, PatternQueryResults patternQueryResults, String prefix, DataSource dataSource, String outputFolder) {

        QueryParams params = extractQueryParams(query);
        List<List<List<Sketch>>> matches = patternQueryResults.getMatches();
        long executionTime = patternQueryResults.getExecutionTime();

        try {
            
            // Create matches directory if it doesn't exist
            Path patternMatchesDir = Paths.get(outputFolder,"pattern_matches");
            if (!Files.exists(patternMatchesDir)) {
                Files.createDirectories(patternMatchesDir);
            }

            Path datasetDir = Paths.get(outputFolder, "pattern_matches", dataSource.getDataset().getTableName());
            if (!Files.exists(datasetDir)) {
                Files.createDirectories(datasetDir);
            }

            // Create specific directory if it doesn't exist
            Path logDir = Paths.get(outputFolder,"pattern_matches",  dataSource.getDataset().getTableName(), prefix);
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
            }
            
            // Create a unique filename with timestamp
            String filename = String.format("%s_%s_%s_%d_%s.log", 
                prefix, params.from, params.to, params.measure, params.timeUnit.toString());
            
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
                    
                    // Calculate the overall match time range
                    long matchStart = Long.MAX_VALUE;
                    long matchEnd = Long.MIN_VALUE;
                    
                    for (List<Sketch> segment : match) {
                        Sketch combinedSketch = combineSketches(segment);
                        matchStart = Math.min(matchStart, combinedSketch.getFrom());
                        matchEnd = Math.max(matchEnd, combinedSketch.getTo());
                    }
                    
                    writer.write(String.format("Match #%d: [%d to %d]\n", matchIdx + 1, matchStart, matchEnd));
                    
                    for (int segmentIdx = 0; segmentIdx < match.size(); segmentIdx++) {
                        List<Sketch> segment = match.get(segmentIdx);
                        Sketch combinedSketch = combineSketches(segment);
                        
                        writer.write(String.format("  Segment %d: [%d to %d]\n", 
                            segmentIdx, combinedSketch.getFrom(), combinedSketch.getTo()));
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
     * Adds an aggregated data point to the appropriate sketch using direct index calculation.
     * If the aggregated data point has count=0, it means there's no data for that interval
     * in the underlying database, and we mark the sketch accordingly.
     * 
     * @param from The start timestamp of the entire range (aligned)
     * @param to The end timestamp of the entire range (aligned)
     * @param timeUnit The time unit of the sketches
     * @param sketches The list of sketches covering the time range
     * @param aggregatedDataPoint The data point to add to the appropriate sketch
     */
    public static void addAggregatedDataPointToSketches(long from, long to, AggregateInterval timeUnit, 
                                                List<Sketch> sketches, AggregatedDataPoint aggregatedDataPoint, ViewPort viewPort) {

        long timestamp = aggregatedDataPoint.getTimestamp();
        // Calculate the sketch index based on the timeUnit
        int index = DateTimeUtil.indexInInterval(from, to, timeUnit, timestamp);
        // Handle the edge case where timestamp is exactly at the end of the range
        if (timestamp == to) {
            // Add to the last sketch
            sketches.get(sketches.size() - 1).addAggregatedDataPoint(aggregatedDataPoint);
            return;
        }
        // Get the appropriate sketch and add the data point
        if (index >= 0 && index < sketches.size()) {
            sketches.get(index).addAggregatedDataPoint(aggregatedDataPoint);
        } else {
            LOG.error("Index calculation error: Computed index {} for timestamp {} is out of bounds (sketches size: {})", 
                    index, timestamp, sketches.size());
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
}
