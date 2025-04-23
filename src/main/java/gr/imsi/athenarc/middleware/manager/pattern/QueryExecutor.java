package gr.imsi.athenarc.middleware.manager.pattern;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.manager.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.manager.pattern.util.Util;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;


public class QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;

    public QueryExecutor(DataSource dataSource, TimeSeriesCache cache) {
        this.dataSource = dataSource;
        this.cache = cache;
    }

    public PatternQueryResults executeQuery(PatternQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasures()[0]; // for now pattern querys have only one measure
        AggregateInterval timeUnit = query.getTimeUnit();
        
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        Set<String> aggregateFunctions = new HashSet<>();
        aggregateFunctions.add("first");
        aggregateFunctions.add("last");
        aggregateFunctions.add("min");
        aggregateFunctions.add("max");
        
        // Align start and end times to the time unit boundaries for proper alignment
        long alignedFrom = alignToTimeUnitBoundary(from, timeUnit, true);  // floor
        long alignedTo = alignToTimeUnitBoundary(to, timeUnit, false);     // ceiling
        
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        
        TimeRange alignedTimeRange = new TimeRange(alignedFrom, alignedTo);
        
        // 1. Create sketches based on the query's timeUnit first, properly aligned
        List<Sketch> sketches = Util.generateAlignedSketches(alignedFrom, alignedTo, timeUnit);
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), timeUnit);
        
        // 2. Check cache for existing data - use compatible spans
        List<TimeSeriesSpan> existingSpans = cache.getCompatibleSpans(measure, alignedTimeRange, timeUnit);
        
        if (!existingSpans.isEmpty()) {
            LOG.info("Found {} existing compatible spans in cache for measure {}", existingSpans.size(), measure);
            
            // 3. Fill sketches with data from cache
            for (TimeSeriesSpan span : existingSpans) {
                if(span instanceof AggregateTimeSeriesSpan) {
                    AggregateTimeSeriesSpan aggregateSpan = (AggregateTimeSeriesSpan) span;
                    Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator();
                    while (dataPoints.hasNext()) {
                        AggregatedDataPoint point = dataPoints.next();
                        addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                    }
                }
            }
        }
        
        // 4. Identify unfilled sketches/intervals, maintaining alignment
        List<TimeInterval> missingIntervals = identifyMissingIntervals(sketches, alignedFrom, alignedTo, timeUnit);
        
        // 5. Fetch data for missing intervals
        if (!missingIntervals.isEmpty()) {
            LOG.info("Identified {} missing intervals that need to be fetched", missingIntervals.size());
            
            // For better performance, merge adjacent intervals while preserving alignment
            List<TimeInterval> mergedMissingIntervals = DateTimeUtil.groupIntervals(timeUnit, missingIntervals);
            LOG.info("Merged into {} intervals for fetching", mergedMissingIntervals.size());
            
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
            missingIntervalsPerMeasure.put(measure, mergedMissingIntervals);
            
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
            aggregateIntervalsPerMeasure.put(measure, timeUnit);
            
            // Fetch missing data, ensuring we use the same time unit for alignment
            AggregatedDataPoints newDataPoints = dataSource.getAggregatedDataPoints(
                alignedFrom, alignedTo, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
                        
            // Create spans and add to cache
            Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
                TimeSeriesSpanFactory.createAggregate(newDataPoints, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);

            for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
                cache.addToCache(spans);
                // Fill the sketches with the new data
                for (TimeSeriesSpan span : spans) {
                    if(span instanceof AggregateTimeSeriesSpan) {
                        AggregateTimeSeriesSpan aggregateSpan = (AggregateTimeSeriesSpan) span;
                        Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator();
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
  
        // Continue with pattern search on the appropriately filled sketches
        long startTime = System.currentTimeMillis();
        LOG.info("Starting search, over {} aggregate data.", sketches.size());
        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes);

        List<List<List<Sketch>>> matches = sketchSearch.findAllMatches();
        long endTime = System.currentTimeMillis();
        LOG.info("Search took {} ms", endTime - startTime);
        LOG.info("Found {} matches", matches.size());

        if (!matches.isEmpty()) {
            for (List<List<Sketch>> firstMatch : matches) {
                LOG.info("Match:");
                for (int i = 0; i < firstMatch.size(); i++) {
                    List<Sketch> segment = firstMatch.get(i);
                    Sketch combinedSketch = Util.combineSketches(segment);
                    LOG.info("Segment {}: {}", i, combinedSketch);
                }
                LOG.info("");
            }
        }
        
        // For example:
        LOG.info("Executing pattern query ");
        PatternQueryResults patternQueryResults = new PatternQueryResults();
        return patternQueryResults;
    }

    /**
     * Aligns a timestamp to the nearest time unit boundary.
     * 
     * @param timestamp The timestamp to align
     * @param timeUnit The time unit to align to
     * @param floor If true, align to floor (start of unit), otherwise ceiling (end of unit)
     * @return The aligned timestamp
     */
    private long alignToTimeUnitBoundary(long timestamp, AggregateInterval timeUnit, boolean floor) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = Instant.ofEpochMilli(timestamp);
        
        // For chronological units like DAYS, HOURS, etc., use Java's truncatedTo
        ChronoUnit chronoUnit = timeUnit.getChronoUnit();
        long multiplier = timeUnit.getMultiplier();
        
        if (multiplier == 1) {
            // Simple case - just truncate to the unit boundary
            if (floor) {
                return instant.atZone(zone).truncatedTo(chronoUnit).toInstant().toEpochMilli();
            } else {
                // For ceiling, go to next unit and subtract 1ms
                return instant.atZone(zone)
                        .truncatedTo(chronoUnit)
                        .plus(1, chronoUnit)
                        .toInstant().toEpochMilli();
            }
        } else {
            // For multiples (e.g., 15 minutes), need special handling
            switch (chronoUnit) {
                case MINUTES:
                    return alignToMultipleOf(timestamp, 60 * 1000, multiplier, floor);
                case HOURS:
                    return alignToMultipleOf(timestamp, 3600 * 1000, multiplier, floor);
                case DAYS:
                    return alignToMultipleOf(timestamp, 24 * 3600 * 1000, multiplier, floor);
                // Add more cases as needed
                default:
                    LOG.warn("Unsupported chrono unit for alignment: {}", chronoUnit);
                    return timestamp;
            }
        }
    }
    
    /**
     * Aligns a timestamp to a multiple of a base unit.
     * 
     * @param timestamp The timestamp to align
     * @param baseUnitMs The base unit in milliseconds (e.g., 60*1000 for minutes)
     * @param multiplier The multiplier (e.g., 15 for 15 minutes)
     * @param floor If true, round down, otherwise round up
     * @return The aligned timestamp
     */
    private long alignToMultipleOf(long timestamp, long baseUnitMs, long multiplier, boolean floor) {
        // Get the epoch second of the day
        long msOfDay = timestamp % (24 * 3600 * 1000);
        long dayStart = timestamp - msOfDay;
        
        // Calculate how many complete units fit
        long unitsElapsed = msOfDay / (baseUnitMs * multiplier);
        
        if (floor) {
            // For floor, just multiply by complete units
            return dayStart + (unitsElapsed * baseUnitMs * multiplier);
        } else {
            // For ceiling, add one more unit if there's a remainder
            if (msOfDay % (baseUnitMs * multiplier) > 0) {
                unitsElapsed++;
            }
            return dayStart + (unitsElapsed * baseUnitMs * multiplier);
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
    private List<TimeInterval> identifyMissingIntervals(List<Sketch> sketches, long from, long to, AggregateInterval timeUnit) {
        List<TimeInterval> missingIntervals = new ArrayList<>();
        long unitDurationMs = timeUnit.toDuration().toMillis();
        
        for (int i = 0; i < sketches.size(); i++) {
            Sketch sketch = sketches.get(i);
            
            // If sketch has no data points at all, we need to check if data exists
            if (!sketch.hasZeroCountPoint()) {
                // Calculate interval boundaries to maintain alignment
                long sketchStart = from + (i * unitDurationMs);
                long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
                
                // Ensure the interval aligns with time unit boundaries
                missingIntervals.add(new TimeRange(sketchStart, sketchEnd));
            }
        }
        
        return missingIntervals;
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
    private void addAggregatedDataPointToSketches(long from, long to, AggregateInterval timeUnit, 
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
            // If the data point has count=0, it means there's no data for this interval
            // in the underlying database, so we mark the sketch accordingly
            sketches.get(index).addAggregatedDataPoint(aggregatedDataPoint);
            
            // If count is 0, mark this sketch as having a zero-count point
            if (aggregatedDataPoint.getCount() == 0) {
                sketches.get(index).markAsZeroCount();
            }
        } else {
            LOG.error("Index calculation error: Computed index {} for timestamp {} is out of bounds (sketches size: {})", 
                    index, timestamp, sketches.size());
        }
    }

}
