package gr.imsi.athenarc.visual.middleware.patterncache;


import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.domain.TimeSeriesSpan;
import gr.imsi.athenarc.visual.middleware.domain.TimeSeriesSpanFactory;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;

public class PatternCache {

    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);

    DataSource dataSource;

    public PatternCache(DataSource dataSource){
        this.dataSource = dataSource;
    }


    public void executeQuery(PatternQuery query){
        long from = query.getFrom();
        long to = query.getTo();
        TimeInterval missingInterval = new TimeRange(from, to);
        int measure = query.getMeasure();
        ChronoUnit chronoUnit = query.getChronoUnit();
        
        ChronoUnit subChronoUnit = ChronoUnit.HOURS;
        List<SegmentSpecification> segmentSpecifications = query.getSegmentSpecifications();
        
        AggregatedDataPoints aggregatedDataPoints = dataSource.getAggregatedDataPoints(from, to, measure, subChronoUnit);
        TimeSeriesSpan timeSeriesSpan = TimeSeriesSpanFactory.createAggregate(aggregatedDataPoints, missingInterval, measure, 3600000L);
        
        // Create sketches based on the query's timeUnit
        List<Sketch> sketches = generateSketches(from, to, chronoUnit);

        Iterator<AggregatedDataPoint> iterator = timeSeriesSpan.iterator(from, to);
        while (iterator.hasNext()) {
            AggregatedDataPoint aggregatedDataPoint = iterator.next();
            addAggregatedDataPointToSketches(from, to, chronoUnit, sketches, aggregatedDataPoint);
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

    /**
     * Generates a list of sketches, each representing one interval of the specified time unit
     * between the given timestamps.
     * 
     * @param from The start timestamp (epoch milliseconds)
     * @param to The end timestamp (epoch milliseconds)
     * @param timeUnit The time unit to divide the interval by (DAYS, HOURS, MINUTES, etc.)
     * @return A list of Sketch objects, each spanning exactly one interval of the specified time unit
     */
    public List<Sketch> generateSketches(long from, long to, ChronoUnit chronoUnit) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Convert epoch milliseconds to Instant
        Instant startInstant = Instant.ofEpochMilli(from);
        Instant endInstant = Instant.ofEpochMilli(to);
        ZoneId zone = ZoneId.systemDefault();
                
        // Start with the truncated time unit (beginning of the time unit period)
        Instant currentInstant = startInstant
            .atZone(zone)
            .truncatedTo(chronoUnit)
            .toInstant();
        
        // Generate sketches for each time unit interval
        while (!currentInstant.isAfter(endInstant)) {
            Instant nextInstant = currentInstant.plus(1, chronoUnit);
            
            // If we're past the end time, use the end time as the boundary
            Instant intervalEnd = nextInstant.isAfter(endInstant) ? endInstant : nextInstant;
            
            sketches.add(new Sketch(
                currentInstant.toEpochMilli(),
                intervalEnd.toEpochMilli()
            ));
            
            currentInstant = nextInstant;
        }
        return sketches;
    }
}
