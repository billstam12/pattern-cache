package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
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
import gr.imsi.athenarc.middleware.pattern.util.Util;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;

public class PatternUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PatternUtils.class);

    public static PatternQueryResults executePatternQuery(PatternQuery query, DataSource dataSource){
        PatternQueryResults patternQueryResults = new PatternQueryResults();
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
                
        // 1. Create sketches based on the query's timeUnit first, properly aligned
        List<Sketch> sketches = Util.generateAlignedSketches(alignedFrom, alignedTo, timeUnit, aggregationType);
        LOG.info("Created {} sketches for aligned time range with time unit {}", 
                sketches.size(), timeUnit);

        // 2. Create a time range for the pattern query
        TimeRange patternTimeRange = new TimeRange(alignedFrom, alignedTo);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
        List<TimeInterval> missingIntervals = new ArrayList<>();  
        missingIntervals.add(patternTimeRange);
        missingIntervalsPerMeasure.put(measure, missingIntervals);

        // 3. Create aggregate intervals for each measure
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = new HashMap<>();
        aggregateIntervalsPerMeasure.put(measure, timeUnit);

        // 4. Get data from the data source
        AggregatedDataPoints newDataPoints = dataSource.getM4DataPoints(
                alignedFrom, alignedTo, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
                        
        // 5. Create spans and add to sketches
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = 
            TimeSeriesSpanFactory.createAggregate(newDataPoints, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);

        for (List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
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

        // 6. Search
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
        return patternQueryResults;
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
}
