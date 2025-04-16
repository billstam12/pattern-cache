package gr.imsi.athenarc.middleware.manager.pattern;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.manager.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.manager.pattern.util.Util;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;

    public QueryExecutor(DataSource dataSource, TimeSeriesCache cache) {
        this.dataSource = dataSource;
        this.cache = cache;
    }

    public List<List<List<Sketch>>> executeQuery(PatternQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasures()[0]; // for now pattern querys have only one measure
        ChronoUnit chronoUnit = query.getChronoUnit();
        ChronoUnit subChronoUnit = ChronoUnit.HOURS;
        AggregateInterval aggregateInterval = new AggregateInterval(1, subChronoUnit);
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        Set<String> aggregateFunctions = new HashSet<>();
        aggregateFunctions.add("first");
        aggregateFunctions.add("last");
        
        List<TimeInterval> missingIntervals = new ArrayList<>();
        TimeRange timeRange = new TimeRange(from, to);
        missingIntervals.add(timeRange);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
        missingIntervalsPerMeasure.put(measure, missingIntervals);
        
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, aggregateInterval);

        // Check cache for existing data
        List<TimeSeriesSpan> existingSpans = cache.getOverlappingSpans(measure, timeRange);
        
        // If we have spans in cache that can be used, process them
        if (!existingSpans.isEmpty()) {
            LOG.info("Found {} existing spans in cache for measure {}", existingSpans.size(), measure);
            // Process existing spans (implementation details would depend on your pattern query logic)
            // ...
        }
        
        // Fetch data not in cache
        AggregatedDataPoints aggregatedDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
        
        // Create spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(aggregatedDataPoints, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
        for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
            cache.addToCache(spans);
        }

        // Create sketches based on the query's timeUnit
        List<Sketch> sketches = Util.generateSketches(from, to, chronoUnit);

        Iterator<AggregatedDataPoint> iterator = aggregatedDataPoints.iterator();
        while (iterator.hasNext()) {
            AggregatedDataPoint aggregatedDataPoint = iterator.next();
            addAggregatedDataPointToSketches(from, to, chronoUnit, sketches, aggregatedDataPoint);
        }
        
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
        return matches;
    }

    /**
     * Adds an aggregated data point to the appropriate sketch using direct index calculation.
     * This is more efficient than searching through all sketches one by one.
     * 
     * @param from The start timestamp of the entire range
     * @param to The end timestamp of the entire range
     * @param sketches The list of sketches covering the time range
     * @param chronoUnit The time unit of the sketches
     * @param aggregatedDataPoint The data point to add to the appropriate sketch
     */
    private void addAggregatedDataPointToSketches(long from, long to, ChronoUnit chronoUnit, List<Sketch> sketches, AggregatedDataPoint aggregatedDataPoint) {
        long timestamp = aggregatedDataPoint.getTimestamp();
        
        // Handle the edge case where timestamp is exactly at the end of the range
        if (timestamp == to) {
            // Add to the last sketch
            sketches.get(sketches.size() - 1).addAggregatedDataPoint(aggregatedDataPoint);
            return;
        }

        // Calculate the sketch index based on the chronoUnit
        Instant startInstant = Instant.ofEpochMilli(from);
        Instant pointInstant = Instant.ofEpochMilli(timestamp);
        ZoneId zone = ZoneId.systemDefault();
        
        // Truncate both instants to the chronoUnit to get aligned boundaries
        Instant truncatedStart = startInstant.atZone(zone).truncatedTo(chronoUnit).toInstant();
        Instant truncatedPoint = pointInstant.atZone(zone).truncatedTo(chronoUnit).toInstant();
        
        // Calculate how many chronoUnits between the start and the data point
        long unitsBetween = chronoUnit.between(truncatedStart, truncatedPoint);
        
        // Get the appropriate sketch and add the data point
        int index = (int) unitsBetween;
        if (index >= 0 && index < sketches.size()) {
            sketches.get(index).addAggregatedDataPoint(aggregatedDataPoint);
        } else {
            LOG.error("Index calculation error: Computed index " + index + 
                    " for timestamp " + timestamp + " is out of bounds (sketches size: " + 
                    sketches.size() + ")");
        }

    }

}
