package gr.imsi.athenarc.middleware.cache;

import java.time.temporal.ChronoUnit;
import java.util.Iterator;

import org.junit.Test;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.IntervalTree;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reproduces the cache miss seen in the predicted run: two spans with the
 * same [from, to) but different aggregateInterval collide in the IntervalTree's
 * compareTo, so insert(secondSpan) returns false and the span never enters the
 * tree even though TimeSeriesCache.addToCache logs it as added.
 */
public class IntervalTreeSpanCollisionTest {

    @Test
    public void sameRangeDifferentAggregateInsertsBothSpans() {
        long from = 1704067200000L;
        long to = 2019686400000L;

        MinMaxAggregateTimeSeriesSpan coarse = new MinMaxAggregateTimeSeriesSpan(
                from, to, 0, AggregateInterval.of(6, ChronoUnit.HOURS));
        MinMaxAggregateTimeSeriesSpan fine = new MinMaxAggregateTimeSeriesSpan(
                from, to, 0, AggregateInterval.of(5, ChronoUnit.MINUTES));

        IntervalTree<TimeSeriesSpan> tree = new IntervalTree<>();
        assertTrue("first insert should succeed", tree.insert(coarse));
        assertTrue("second insert (different aggInterval, same range) should also succeed",
                tree.insert(fine));

        int count = 0;
        Iterator<TimeSeriesSpan> it = tree.overlappers(new TimeRange(from, to));
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals("both spans should be retrievable from the tree", 2, count);
    }

    @Test
    public void visualLookupReturnsOnlyCoarsestAdmissibleAggregated() {
        long from = 1704067200000L;
        long to = 2019686400000L;
        TimeSeriesCache cache = new TimeSeriesCache();

        MinMaxAggregateTimeSeriesSpan twelveHour = new MinMaxAggregateTimeSeriesSpan(
                from, to, 0, AggregateInterval.of(12, ChronoUnit.HOURS));
        MinMaxAggregateTimeSeriesSpan sixHour = new MinMaxAggregateTimeSeriesSpan(
                from, to, 0, AggregateInterval.of(6, ChronoUnit.HOURS));
        MinMaxAggregateTimeSeriesSpan fiveMin = new MinMaxAggregateTimeSeriesSpan(
                from, to, 0, AggregateInterval.of(5, ChronoUnit.MINUTES));

        cache.addToCache(twelveHour);
        cache.addToCache(sixHour);
        cache.addToCache(fiveMin);

        AggregateInterval pixelColumnInterval = AggregateInterval.of(4, ChronoUnit.DAYS);
        java.util.List<TimeSeriesSpan> spans = cache.getOverlappingSpansForVisualization(
                0, new TimeRange(from, to), pixelColumnInterval);

        assertEquals("only the coarsest admissible aggregated span should be returned", 1, spans.size());
        assertEquals(twelveHour.getAggregateInterval(), spans.get(0).getAggregateInterval());
    }
}
