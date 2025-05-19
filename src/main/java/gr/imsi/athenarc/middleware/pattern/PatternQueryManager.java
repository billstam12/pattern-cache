package gr.imsi.athenarc.middleware.pattern;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.pattern.nfa.NFASketchSearch;
import gr.imsi.athenarc.middleware.pattern.util.Util;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;


public class PatternQueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache) {
        this.dataSource = dataSource;
        this.cache = cache;
    }

    public PatternQueryResults executeQuery(PatternQuery query) {
        long startTime = System.currentTimeMillis();
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasures().get(0); // for now pattern querys have only one measure
        AggregateInterval timeUnit = query.getTimeUnit();
        AggregationType aggregationType = query.getAggregationType();
        
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        // Align start and end times to the time unit boundaries for proper alignment
        long alignedFrom = DateTimeUtil.alignToTimeUnitBoundary(from, timeUnit, true);  // floor
        long alignedTo = DateTimeUtil.alignToTimeUnitBoundary(to, timeUnit, false);     // ceiling
        
        LOG.info("Original time range: {} to {}", from, to);
        LOG.info("Aligned time range: {} to {} with time unit {}", alignedFrom, alignedTo, timeUnit);
        
        TimeRange alignedTimeRange = new TimeRange(alignedFrom, alignedTo);
        
        // 1. Create sketches based on the query's timeUnit first, properly aligned
        List<Sketch> sketches = Util.generateAlignedSketches(alignedFrom, alignedTo, timeUnit, aggregationType);
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
                    Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                    while (dataPoints.hasNext()) {
                        AggregatedDataPoint point = dataPoints.next();
                        PatternUtils.addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                    }
                }
            }
        }
        
        // 4. Identify unfilled sketches/intervals, maintaining alignment
        List<TimeInterval> missingIntervals = identifyMissingIntervals(sketches, alignedFrom, alignedTo, timeUnit);
        
        // 5. Fetch data for missing intervals
        if (!missingIntervals.isEmpty()) {
            // For better performance, merge adjacent intervals while preserving alignment
            List<TimeInterval> mergedMissingIntervals = DateTimeUtil.groupIntervals(timeUnit, missingIntervals);
            LOG.info("Merged into {} intervals for fetching", mergedMissingIntervals.size());
            
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
            missingIntervalsPerMeasure.put(measure, mergedMissingIntervals);
            
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
            aggregateIntervalsPerMeasure.put(measure, timeUnit);
            
            Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure = DateTimeUtil.alignIntervalsToTimeUnitBoundary(missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);

            // Fetch missing data, ensuring we use the same time unit for alignment
            AggregatedDataPoints newDataPoints = dataSource.getM4DataPoints(
                alignedFrom, alignedTo, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);
                        
            // Create spans and add to cache
            Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
                TimeSeriesSpanFactory.createAggregate(newDataPoints, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);

            for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
                cache.addToCache(spans);
                // Fill the sketches with the new data
                for (TimeSeriesSpan span : spans) {
                    if(span instanceof AggregateTimeSeriesSpan) {
                        AggregateTimeSeriesSpan aggregateSpan = (AggregateTimeSeriesSpan) span;
                        Iterator<AggregatedDataPoint> dataPoints = aggregateSpan.iterator(alignedFrom, alignedTo);
                        while (dataPoints.hasNext()) {
                            AggregatedDataPoint point = dataPoints.next();
                            PatternUtils.addAggregatedDataPointToSketches(alignedFrom, alignedTo, timeUnit, sketches, point);
                        }
                    }
                }
            }
        
            
        } else {
            LOG.info("All required data available in cache, no need for additional fetching");
        }

        // Continue with pattern search on the appropriately filled sketches
        LOG.info("Starting search, over {} aggregate data.", sketches.size());
        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes);
        // SketchSearch sketchSearch = new SketchSearch(sketches, patternNodes);

        List<List<List<Sketch>>> matches = sketchSearch.findAllMatches();
        long endTime = System.currentTimeMillis();
        LOG.info("Search took {} ms", endTime - startTime);
        LOG.info("Found {} matches", matches.size());

        if (!matches.isEmpty()) {
            for (List<List<Sketch>> firstMatch : matches) {
                LOG.debug("Match:");
                for (int i = 0; i < firstMatch.size(); i++) {
                    List<Sketch> segment = firstMatch.get(i);
                    Sketch combinedSketch = Util.combineSketches(segment);
                    LOG.debug("Segment {}: {}", i, combinedSketch);
                }
                LOG.debug("");
            }
        }
        
        PatternQueryResults patternQueryResults = new PatternQueryResults();
        return patternQueryResults;
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
