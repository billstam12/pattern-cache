package gr.imsi.athenarc.visual.middleware;

import org.junit.Test;
import static org.junit.Assert.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.ArrayList;

import gr.imsi.athenarc.visual.middleware.patterncache.Sketch;
import gr.imsi.athenarc.visual.middleware.patterncache.SketchSearch;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.util.Util;

// Dummy AggregatedDataPoint implementation for testing.
class DummyAggregatedDataPoint implements gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint {
    private final long timestamp;
    private final double value;
    
    public DummyAggregatedDataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public double getValue() {
        return value;
    }
    
    // For simplicity, we assume count is 1 and first/last datapoints are this datapoint.
    @Override
    public int getCount() {
        return 1;
    }
    
    @Override
    public Stats getStats() {
        // Create a dummy Stats that uses the same value and timestamp for first and last.
        StatsAggregator stats = new StatsAggregator();
        stats.accept(new ImmutableDataPoint(timestamp, value, -1));
        stats.accept(new ImmutableDataPoint(timestamp, value, -1));
        return stats;
    }
    
    @Override
    public int getMeasure() {
        return -1;
    }

    @Override
    public long getFrom() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFrom'");
    }

    @Override
    public long getTo() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTo'");
    }
}

public class UnitTests {

    /**
     * Test that combining two consecutive sketches aggregates the value as expected.
     * For example, if we create sketches with dummy data points that result in a known slope,
     * then the normalized value (0.5 + atan(slope)/PI) should be computed correctly.
     */
    @Test
    public void testSketchCombineAndGetValue() {
        // Create two consecutive sketches.
        Sketch sketch1 = new Sketch(0, 1000);
        Sketch sketch2 = new Sketch(1000, 2000);
        
        // Simulate aggregated data points.
        // For simplicity, add a data point with a known timestamp and value.
        // You may adjust the values such that the computed slope is known.
        DummyAggregatedDataPoint dp1 = new DummyAggregatedDataPoint(0, 10.0);
        DummyAggregatedDataPoint dp2 = new DummyAggregatedDataPoint(1000, 20.0);
        
        sketch1.addAggregatedDataPoint(dp1);
        sketch2.addAggregatedDataPoint(dp2);
        
        // Combine sketches; since sketch1.getTo() equals sketch2.getFrom(), they should combine.
        sketch1.combine(sketch2);
        
        // Now compute the expected slope.
        // In this dummy case, slope = (20.0 - 10.0) / (1000 - 0) = 10/1000 = 0.01.
        double slope = 0.01;
        double expectedNormalized = 0.5 + Math.atan(slope) / Math.PI;
        double actualValue = sketch1.getValue();
        
        assertEquals("Combined sketch value should match expected normalized slope", 
                     expectedNormalized, actualValue, 0.0001);
    }
    
    /**
     * Test that combining non-consecutive sketches does not change the sketch.
     * This should log an error and not combine.
     */
    @Test
    public void testSketchCombineNonConsecutive() {
        Sketch sketch1 = new Sketch(0, 1000);
        Sketch sketch2 = new Sketch(1500, 2500);  // Non consecutive
        
        // Add dummy aggregated data points.
        DummyAggregatedDataPoint dp1 = new DummyAggregatedDataPoint(0, 10.0);
        DummyAggregatedDataPoint dp2 = new DummyAggregatedDataPoint(1500, 20.0);
        sketch1.addAggregatedDataPoint(dp1);
        sketch2.addAggregatedDataPoint(dp2);
        
        // Try combining. According to your code, it should log an error.
        sketch1.combine(sketch2);
        
        // Since combine did not occur, the to should remain unchanged.
        assertEquals("Sketch to should remain unchanged when combining non-consecutive sketches", 
                     1000, sketch1.getTo());
    }
    
    /**
     * Test the PatternCache.generateSketches method to ensure it creates the correct number of sketches.
     */
    @Test
    public void testGenerateSketches() {        
        // Define a time range that spans 3 days.
        Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
        Instant end = start.plus(3, ChronoUnit.DAYS);
        
        List<Sketch> sketches = Util.generateSketches(start.toEpochMilli(), end.toEpochMilli(), ChronoUnit.DAYS);
        // Expect 4 sketches if the end is inclusive (depending on your implementation).
        // Adjust the expected count if your method is designed differently.
        assertEquals("generateSketches should create the correct number of sketches", 
                     4, sketches.size());
    }
    
    /**
     * Test adding aggregated data points to the appropriate sketch in PatternCache.
     * This simulates the addAggregatedDataPointToSketches method.
     */
    @Test
    public void testAddAggregatedDataPointToSketches() {
        Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
        Instant end = start.plus(1, ChronoUnit.DAYS);
        
        List<Sketch> sketches = Util.generateSketches(start.toEpochMilli(), end.toEpochMilli(), ChronoUnit.HOURS);
        // Create a dummy aggregated data point exactly in the middle of the day.
        long midTimestamp = start.plus(12, ChronoUnit.HOURS).toEpochMilli();
        DummyAggregatedDataPoint dp = new DummyAggregatedDataPoint(midTimestamp, 50.0);
        
        int index = 12;
        sketches.get(index).addAggregatedDataPoint(dp);
        
        // Now verify that the sketch at index 12 has a count of 1.
        assertEquals("Sketch at index 12 should have one aggregated data point", 
                     1, sketches.get(index).getCount());
    }
    
    /**
     * Integration test for SketchSearch.
     * Create a PatternQuery with a segment specification and a set of sketches
     * that, when combined, produce a composite value within the desired range.
     */
    @Test
    public void testMultipleSegmentsIntegration() {
        // Create 4 sketches. The actual from/to values are not critical since each sketch is 1 unit long.
        List<Sketch> sketches = new ArrayList<>();
        sketches.add(new Sketch(0, 1000));  // Sketch 0
        sketches.add(new Sketch(1000, 2000));  // Sketch 1
        sketches.add(new Sketch(2000, 3000));  // Sketch 2
        sketches.add(new Sketch(3000, 4000));  // Sketch 3

        // Segment 1: add dummy data points
        DummyAggregatedDataPoint dp1 = new DummyAggregatedDataPoint(0, 10.0);
        DummyAggregatedDataPoint dp2 = new DummyAggregatedDataPoint(1000, 20.0);
        sketches.get(0).addAggregatedDataPoint(dp1);
        sketches.get(1).addAggregatedDataPoint(dp2);

        // Segment 2: add dummy data points
        DummyAggregatedDataPoint dp3 = new DummyAggregatedDataPoint(2000, 30.0);
        DummyAggregatedDataPoint dp4 = new DummyAggregatedDataPoint(3000, 50.0);
        sketches.get(2).addAggregatedDataPoint(dp3);
        sketches.get(3).addAggregatedDataPoint(dp4);

        // Define SegmentSpecification for Segment 1: require exactly 2 sketches.
        TimeFilter tf1 = new TimeFilter(false, 2, 2);
        // Expect normalized value around 0.5032; set a narrow acceptable range.
        ValueFilter vf1 = new ValueFilter(false, 0.503, 0.504);
        SegmentSpecification segSpec1 = new SegmentSpecification(tf1, vf1);

        // Define SegmentSpecification for Segment 2: require exactly 2 sketches.
        TimeFilter tf2 = new TimeFilter(false, 2, 2);
        // Expect normalized value around 0.5064; set a narrow acceptable range.
        ValueFilter vf2 = new ValueFilter(false, 0.506, 0.507);
        SegmentSpecification segSpec2 = new SegmentSpecification(tf2, vf2);

        // Combine both segment specifications into a PatternQuery.
        List<SegmentSpecification> segSpecs = new ArrayList<>();
        segSpecs.add(segSpec1);
        segSpecs.add(segSpec2);

        // For PatternQuery parameters, we use arbitrary values for from, to, measure, and time unit.
        long from = 0;
        long to = 4000;
        int measure = 1;
        ChronoUnit chronoUnit = ChronoUnit.DAYS;
        PatternQuery query = new PatternQuery(from, to, measure, chronoUnit, segSpecs);

        // Create the SketchSearch automaton and run the query.
        SketchSearch search = new SketchSearch(sketches, query);
        boolean result = search.run();

        // The test expects that both segments will match their respective filters.
        assertTrue("SketchSearch should return true when both segments match", result);
    }
}
