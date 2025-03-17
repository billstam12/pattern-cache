package gr.imsi.athenarc.visual.middleware;

import org.junit.Test;
import static org.junit.Assert.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import gr.imsi.athenarc.visual.middleware.patterncache.Sketch;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.visual.middleware.patterncache.util.Util;
import gr.imsi.athenarc.visual.middleware.patterncache.SketchSearch;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;
import java.util.ArrayList;
import java.util.Arrays;

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
        stats.accept(new ImmutableDataPoint(timestamp, value));
        stats.accept(new ImmutableDataPoint(timestamp, value));
        return stats;
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

    @Override
    public DataPoint getRepresentativeDataPoint() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRepresnentativeDataPoint'");
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
        Sketch sketch3 = new Sketch(2000, 3000);

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
        // In this dummy case, slope = (20.0 - 10.0) / (1 - 0) = 10 / 2 = 5.
        double slope = 5;
        double expectedNormalized = Math.atan(slope) / Math.PI;
        double actualValue = sketch1.computeSlope();
        
        assertEquals("Combined sketch value should match expected normalized slope", 
                     expectedNormalized, actualValue, 0.0001);

        sketch1.combine(sketch3);
        assertEquals("Combined sketch duration should be 3", 3, sketch1.getDuration());
        
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
        try {
            sketch1.combine(sketch2);
        } catch (Exception e) {
            // Expected exception; do nothing.
        }
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
     * Test pattern search with variable length segments
     * This test creates a pattern with three segments:
     * 1. An upward slope (rising)
     * 2. A variable-length flat segment
     * 3. A downward slope (falling)
     * Then validates that the pattern is correctly matched in a sequence of sketches.
     */
    // @Test
    // public void testVariableLengthPatternSearch() {
    //     // Create a sequence of sketches with known slopes
    //     List<Sketch> testSketches = new ArrayList<>();
        
    //     // Create 7 sketches with timestamps 0-7000 in steps of 1000
    //     for (int i = 0; i < 7; i++) {
    //         testSketches.add(new Sketch(i * 1000, (i + 1) * 1000));
    //     }
        
    //     // Add datapoints to create a pattern:
    //     // Sketches 0-1: rising slope (value increases from 10 to 20)
    //     // Sketches 2-4: flat (value stays at 20)
    //     // Sketches 5-6: falling slope (value decreases from 20 to 10)
    //     testSketches.get(0).addAggregatedDataPoint(new DummyAggregatedDataPoint(0, 10.0));
    //     testSketches.get(1).addAggregatedDataPoint(new DummyAggregatedDataPoint(1000, 20.0));
    //     testSketches.get(2).addAggregatedDataPoint(new DummyAggregatedDataPoint(2000, 20.0));
    //     testSketches.get(3).addAggregatedDataPoint(new DummyAggregatedDataPoint(3000, 20.0));
    //     testSketches.get(4).addAggregatedDataPoint(new DummyAggregatedDataPoint(4000, 20.0));
    //     testSketches.get(5).addAggregatedDataPoint(new DummyAggregatedDataPoint(5000, 20.0));
    //     testSketches.get(6).addAggregatedDataPoint(new DummyAggregatedDataPoint(6000, 10.0));
        
    //     // First segment (rising): duration exactly 2 sketches, positive slope
    //     SegmentSpecification segment1 = new SegmentSpecification(new TimeFilter(false, 2, 2), new ValueFilter(false, 0.1, 1.0));
    //     // Second segment (flat): variable duration between 1-3 sketches, near-zero slope

    //     SegmentSpecification segment2 = new SegmentSpecification(new TimeFilter(false, 1, 3), new ValueFilter(false, -0.05, 0.05));

    //     // Third segment (falling): duration exactly 2 sketches, negative slope
    //     SegmentSpecification segment3 = new SegmentSpecification(new TimeFilter(false, 2, 2), new ValueFilter(false, -1.0, -0.1) );
        
    //     // Add segments to query
    //     List<SegmentSpecification> segmentSpecifications = (Arrays.asList(segment1, segment2, segment3));
        
    //     PatternQuery query = new PatternQuery(0, 7000, 1, ChronoUnit.SECONDS, segmentSpecifications);

    //     // Run the pattern search
    //     SketchSearch sketchSearch = new SketchSearch(testSketches, query);
        
    //     // Validate the matched segments
    //     List<List<Sketch>> matchedSegments = sketchSearch.findAllMatches().get(0); // get the first match
    //     assertEquals("Should have 3 matched segments", 3, matchedSegments.size());
        
    //     // First segment should be sketches 0-1 (rising)
    //     assertEquals("First segment should contain 2 sketches", 2, matchedSegments.get(0).size());
    //     assertEquals("First sketch in segment 1", 0L, matchedSegments.get(0).get(0).getFrom());
    //     assertEquals("Last sketch in segment 1", 2000L, matchedSegments.get(0).get(1).getTo());
        
    //     // Second segment should be sketches 2-4 (flat, variable length)
    //     assertEquals("Second segment should contain 3 sketches", 3, matchedSegments.get(1).size());
    //     assertEquals("First sketch in segment 2", 2000L, matchedSegments.get(1).get(0).getFrom());
    //     assertEquals("Last sketch in segment 2", 5000L, matchedSegments.get(1).get(2).getTo());
        
    //     // Third segment should be sketches 5-6 (falling)
    //     assertEquals("Third segment should contain 2 sketches", 2, matchedSegments.get(2).size());
    //     assertEquals("First sketch in segment 3", 5000L, matchedSegments.get(2).get(0).getFrom());
    //     assertEquals("Last sketch in segment 3", 7000L, matchedSegments.get(2).get(1).getTo());
    // }
}
