package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.M4AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;

public class PatternUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PatternUtils.class);

    /**
     * Generate sketches covering the specified time range based on the AggregateInterval.
     * Ensures the sketches are properly aligned with chronological boundaries.
     * 
     * @param from Start timestamp (already aligned to time unit boundary)
     * @param to End timestamp (already aligned to time unit boundary)
     * @param timeUnit Aggregate interval for sketches
     * @param aggregationType Type of aggregation for the sketches
     * @param hasTimestamps Whether the sketches should include timestamps
     * @return List of sketches spanning the time range
     */
    public static List<Sketch> generateAlignedSketches(long from, long to, AggregateInterval timeUnit, AggregationType aggregationType, boolean hasTimestamps) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Calculate the number of complete intervals
        long unitDurationMs = timeUnit.toDuration().toMillis();
        int numIntervals = (int) Math.ceil((double)(to - from) / unitDurationMs);
        
        // Create a sketch for each interval
        for (int i = 0; i < numIntervals; i++) {
            long sketchStart = from + (i * unitDurationMs);
            long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
            Sketch sketch = hasTimestamps ? 
                new TimestampedSketch(sketchStart, sketchEnd, aggregationType) :
                new NonTimestampedSketch(sketchStart, sketchEnd, aggregationType);
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
     * Execute a pattern query with caching support.
     * This method handles cache lookups and updates for pattern matching.
     * 
     * @param query The pattern query to execute
     * @param dataSource The data source to use for fetching missing data
     * @param cache The cache to check for existing data and to update with new data
     * @param aggregateFunctions The set of aggregate functions to use when fetching data
     * @return Pattern query results
     */
    public static PatternQueryResults executePatternQueryWithCache(PatternQuery query, DataSource dataSource, 
                                                                  TimeSeriesCache cache, Set<String> aggregateFunctions) {        
        // Extract query parameters
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        // Create sketches for non-timestamped pattern matching (used with cache)
        List<Sketch> sketches = generateAlignedSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, 
                params.aggregationType, false);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);
        
        // Check cache and populate sketches with existing data
        populateSketchesFromCache(
                sketches, cache, params.measure, 
                params.alignedFrom, params.alignedTo, params.timeUnit);
        
        // Fetch missing data from datasource and update cache
        fetchMissingDataAndUpdateCache(
                sketches, dataSource, cache, 
                params.measure, params.alignedFrom, params.alignedTo, 
                params.timeUnit, aggregateFunctions);
        
        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = performPatternMatching(sketches, patternNodes);

        
        return new PatternQueryResults();
    }

    /**
     * Execute a pattern query directly without using cache.
     * This method fetches all required data directly from the data source.
     * 
     * @param query The pattern query to execute
     * @param dataSource The data source to use for fetching data
     * @return Pattern query results
     */
    public static PatternQueryResults executePatternQuery(PatternQuery query, DataSource dataSource){
        long startTime = System.currentTimeMillis();

        // Extract query parameters
        QueryParams params = extractQueryParams(query);
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        // Create timestamped sketches for direct data source pattern matching
        List<Sketch> sketches = generateAlignedSketches(
                params.alignedFrom, params.alignedTo, params.timeUnit, 
                params.aggregationType, true);
        
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), params.timeUnit);

        // Fetch all data directly from data source
        fetchAllDataFromDataSource(
                sketches, dataSource, params.measure,
                params.alignedFrom, params.alignedTo, params.timeUnit);

        // Perform pattern matching and return results
        List<List<List<Sketch>>> matches = performPatternMatching(sketches, patternNodes);
        
        return new PatternQueryResults();
    }
    
    /**
     * Helper class to store query parameters extracted from a PatternQuery
     */
    private static class QueryParams {
        final long from;
        final long to;
        final long alignedFrom;
        final long alignedTo;
        final int measure;
        final AggregateInterval timeUnit;
        final AggregationType aggregationType;
        
        QueryParams(long from, long to, long alignedFrom, long alignedTo, 
                   int measure, AggregateInterval timeUnit, AggregationType aggregationType) {
            this.from = from;
            this.to = to;
            this.alignedFrom = alignedFrom;
            this.alignedTo = alignedTo;
            this.measure = measure;
            this.timeUnit = timeUnit;
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
        
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        
        return new QueryParams(from, to, alignedFrom, alignedTo, measure, timeUnit, aggregationType);
    }
    
    /**
     * Populate sketches with data from cache if available.
     */
    private static void populateSketchesFromCache(List<Sketch> sketches, TimeSeriesCache cache, 
                                              int measure, long alignedFrom, long alignedTo, 
                                              AggregateInterval timeUnit) {
        if (cache == null) {
            return;
        }
        
        TimeRange alignedTimeRange = new TimeRange(alignedFrom, alignedTo);
        List<TimeSeriesSpan> existingSpans = cache.getCompatibleSpans(measure, alignedTimeRange, timeUnit);
        
        if (!existingSpans.isEmpty()) {
            LOG.info("Found {} existing compatible spans in cache for measure {}", existingSpans.size(), measure);
            
            // Fill sketches with data from cache
            for (TimeSeriesSpan span : existingSpans) {
                if(span instanceof AggregateTimeSeriesSpan) {
                    AggregateTimeSeriesSpan aggregateSpan = (AggregateTimeSeriesSpan) span;
                    Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                    while (dataPoints.hasNext()) {
                        AggregatedDataPoint point = dataPoints.next();
                        addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported span type for cached patterns: " + span.getClass());
                }
            }
        }
    }
    
    /**
     * Fetch missing data from data source and update cache.
     */
    private static void fetchMissingDataAndUpdateCache(List<Sketch> sketches, DataSource dataSource, 
                                                   TimeSeriesCache cache, int measure, 
                                                   long alignedFrom, long alignedTo, 
                                                   AggregateInterval timeUnit, 
                                                   Set<String> aggregateFunctions) {
        // Identify unfilled sketches/intervals
        List<TimeInterval> missingIntervals = identifyMissingIntervals(sketches, alignedFrom, alignedTo, timeUnit);
        
        if (!missingIntervals.isEmpty()) {
            // For better performance, merge adjacent intervals while preserving alignment
            List<TimeInterval> mergedMissingIntervals = DateTimeUtil.groupIntervals(timeUnit, missingIntervals);
            LOG.info("Merged into {} intervals for fetching", mergedMissingIntervals.size());
            
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
            missingIntervalsPerMeasure.put(measure, mergedMissingIntervals);
            
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
            aggregateIntervalsPerMeasure.put(measure, timeUnit);
            
            Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure = 
                DateTimeUtil.alignIntervalsToTimeUnitBoundary(missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);

            // Fetch missing data
            AggregatedDataPoints newDataPoints = dataSource.getAggregatedDataPoints(
                alignedFrom, alignedTo, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
                        
            // Create spans and add to cache
            Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
                TimeSeriesSpanFactory.createAggregate(newDataPoints, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);

            for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
                if (cache != null) {
                    cache.addToCache(spans);
                }
                // Fill the sketches with the new data
                for (TimeSeriesSpan span : spans) {
                    if(span instanceof AggregateTimeSeriesSpan) {
                        AggregateTimeSeriesSpan aggregateSpan = (AggregateTimeSeriesSpan) span;
                        Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                        while (dataPoints.hasNext()) {
                            AggregatedDataPoint point = dataPoints.next();
                            addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                        }
                    }
                }
            }
        } else {
            LOG.info("All required data available in cache, no need for additional fetching");
        }
    }
    
    /**
     * Fetch all required data directly from data source (no caching).
     * Used by the non-cached pattern query execution.
     */
    private static void fetchAllDataFromDataSource(List<Sketch> sketches, DataSource dataSource, 
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
        AggregatedDataPoints newDataPoints = dataSource.getM4DataPoints(
                alignedFrom, alignedTo, intervalsPerMeasure, aggregateIntervalsPerMeasure);
                        
        // Create spans and add to sketches
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
            TimeSeriesSpanFactory.createAggregateM4(newDataPoints, intervalsPerMeasure, aggregateIntervalsPerMeasure);
        
        // Fill the sketches with the data
        for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
            for (TimeSeriesSpan span : spans) {
                if(span instanceof M4AggregateTimeSeriesSpan) {
                    M4AggregateTimeSeriesSpan aggregateSpan = (M4AggregateTimeSeriesSpan) span;
                    Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                    while (dataPoints.hasNext()) {
                        AggregatedDataPoint point = dataPoints.next();
                        addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                    }
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
     * Log pattern matches for debugging purposes
     */
    private static void logMatches(List<List<List<Sketch>>> matches) {
        if (!matches.isEmpty()) {
            for (List<List<Sketch>> match : matches) {
                LOG.debug("Match:");
                for (int i = 0; i < match.size(); i++) {
                    List<Sketch> segment = match.get(i);
                    Sketch combinedSketch = combineSketches(segment);
                    LOG.debug("Segment {}: {}", i, combinedSketch);
                }
                LOG.debug("");
            }
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
                                                List<Sketch> sketches, AggregatedDataPoint aggregatedDataPoint) {
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
    public static List<TimeInterval> identifyMissingIntervals(List<Sketch> sketches, long from, long to, AggregateInterval timeUnit) {
        List<TimeInterval> missingIntervals = new ArrayList<>();
        long unitDurationMs = timeUnit.toDuration().toMillis();
        
        for (int i = 0; i < sketches.size(); i++) {
            Sketch sketch = sketches.get(i);
            
            // If sketch has not initialized, it means we dont have its data in the cache
            if (!sketch.hasInitialized()) {
                // Calculate interval boundaries to maintain alignment
                long sketchStart = from + (i * unitDurationMs);
                long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
                
                // Ensure the interval aligns with time unit boundaries
                missingIntervals.add(new TimeRange(sketchStart, sketchEnd));
            }
        }
        LOG.info("Identified {} missing intervals", missingIntervals.size());
        return missingIntervals;
    }
}
