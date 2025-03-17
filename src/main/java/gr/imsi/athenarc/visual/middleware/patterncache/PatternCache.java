package gr.imsi.athenarc.visual.middleware.patterncache;


import java.util.List;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.patterncache.nfa.NFASketchSearch;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternNode;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.util.Util;

public class PatternCache {

    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);

    DataSource dataSource;

    public PatternCache(DataSource dataSource){
        this.dataSource = dataSource;
    }


    public void executeQuery(PatternQuery query){
        long from = query.getFrom();
        long to = query.getTo();
        int measure = query.getMeasure();
        ChronoUnit chronoUnit = query.getChronoUnit();
        ChronoUnit subChronoUnit = ChronoUnit.HOURS;
        AggregateInterval aggregateInterval = new AggregateInterval(1, subChronoUnit);
        List<PatternNode> patternNodes = query.getPatternNodes();
        AggregatedDataPoints aggregatedDataPoints = dataSource.getSlopeDataPoints(from, to, measure, aggregateInterval);
        
        // Create sketches based on the query's timeUnit
        List<Sketch> sketches = Util.generateSketches(from, to, chronoUnit);

        Iterator<AggregatedDataPoint> iterator = aggregatedDataPoints.iterator();
        while (iterator.hasNext()) {
            AggregatedDataPoint aggregatedDataPoint = iterator.next();
            addAggregatedDataPointToSketches(from, to, chronoUnit, sketches, aggregatedDataPoint);
        }
    
        long startTime = System.currentTimeMillis();
        LOG.info("Starting search, over {} aggregate data.", sketches.size());
        // SketchSearch sketchSearch = new SketchSearch(sketches, patternNodes);
        NFASketchSearch sketchSearch = new NFASketchSearch(sketches, patternNodes);
        // SketchSearchBFS sketchSearch = new SketchSearchBFS(sketches, patternNodes);

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
