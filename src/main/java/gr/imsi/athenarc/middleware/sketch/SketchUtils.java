package gr.imsi.athenarc.middleware.sketch;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.CoveredSpan;
import gr.imsi.athenarc.middleware.cache.RawTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ViewPort;

/** Functions used both for visual and patterns */
public class SketchUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SketchUtils.class);


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
            LOG.debug("Adding aggregated data point with timestamp {} to sketch at index {}", 
                    timestamp, index);
            sketches.get(index).addAggregatedDataPoint(aggregatedDataPoint);
        } else {
            LOG.error("Index calculation error: Computed index {} for timestamp {} is out of bounds (sketches size: {})", 
                    index, timestamp, sketches.size());
        }
    }
    

    public static void populateSketchesFromDataPoints(Iterator<AggregatedDataPoint> dataPoints, 
                                                List<Sketch> sketches, 
                                                long from, 
                                                long to, 
                                                AggregateInterval timeUnit) {
        while (dataPoints != null && dataPoints.hasNext()) {
            AggregatedDataPoint point = dataPoints.next();
            addAggregatedDataPointToSketches(from, to, timeUnit, sketches, point);
        }
    }
    
    /**
     * Populate sketches with data from a TimeSeriesSpan. Raw spans are skipped — only
     * aggregate spans (whose {@code iterator} yields {@code AggregatedDataPoint}) feed
     * sketches; {@code @SuppressWarnings} covers the raw-{@link Iterator} signature on
     * the base interface, which has to stay raw because raw spans return {@code DataPoint}.
     */
    @SuppressWarnings("unchecked")
    public static void populateSketchesFromSpans(List<TimeSeriesSpan> spans,
                                      List<Sketch> sketches,
                                      long from,
                                      long to,
                                      AggregateInterval timeUnit) {
        for (TimeSeriesSpan span : spans) {
            if (span instanceof RawTimeSeriesSpan) {
                continue;
            }
            populateSketchesFromDataPoints(span.iterator(from, to), sketches, from, to, timeUnit);
        }
    }

    /**
     * Populate sketches from a coarsest-per-region cover. Each {@link CoveredSpan}
     * has an owned sub-range; only sub-bucket data points whose {@code [from, to]}
     * is fully inside that owned range get emitted into sketches. Data points
     * outside the owned range belong to a different (or no) covered span and are
     * skipped — this keeps each sketch's {@code allDataPoints} list at a single
     * resolution per time slice and avoids the redundant-overlap bound distortion
     * that mixed-resolution input would cause.
     */
    public static void populateSketchesFromCoveredSpans(List<CoveredSpan> covered,
                                                        List<Sketch> sketches,
                                                        long from,
                                                        long to,
                                                        AggregateInterval timeUnit) {
        populateSketchesFromCoveredSpans(covered, sketches, from, to, timeUnit, false);
    }

    /**
     * Variant that, when {@code relaxedCacheReuse=true}, also keeps sub-buckets
     * that straddle the owned slice boundary or the outer-bucket sketch grid.
     * Straddlers are routed via {@link #addAggregatedDataPointToOverlappingSketches}
     * so each overlapping sketch sees the sub-bucket once (D2 shared-partial
     * discipline; the chained-sketch dedupe in {@code combine()} drops the
     * duplicate at the seam). Rigor is preserved by the relaxed-alignment proof
     * in {@code docs/approxols-bound-proof-relaxed-alignment.md} — the bound is
     * over the sub-bucket-union segment, fuzzed by ≤ one sub-bucket width per
     * side vs. the calendar-aligned segment.
     */
    @SuppressWarnings("unchecked")
    public static void populateSketchesFromCoveredSpans(List<CoveredSpan> covered,
                                                        List<Sketch> sketches,
                                                        long from,
                                                        long to,
                                                        AggregateInterval timeUnit,
                                                        boolean relaxedCacheReuse) {
        // D2 routing requires the sketch type to accept the same physical sub-bucket
        // in two adjacent sketches (chained-sketch seam dedupe then keeps the union
        // tile gap-free). Sketches that accumulate stats into a single running
        // aggregator (e.g. OLSSketch's slope sums) cannot — so the relaxed branch is
        // gated on the type's opt-in {@link Sketch#supportsRelaxedAlignment}.
        boolean relaxedActive = relaxedCacheReuse
                && !sketches.isEmpty()
                && sketches.get(0).supportsRelaxedAlignment();
        for (CoveredSpan cs : covered) {
            TimeSeriesSpan span = cs.getSpan();
            long ownedFrom = cs.getOwnedFrom();
            long ownedTo = cs.getOwnedTo();
            Iterator<AggregatedDataPoint> dataPoints = span.iterator(ownedFrom, ownedTo);
            while (dataPoints != null && dataPoints.hasNext()) {
                AggregatedDataPoint p = dataPoints.next();
                if (p.getFrom() >= ownedFrom && p.getTo() <= ownedTo) {
                    addAggregatedDataPointToSketches(from, to, timeUnit, sketches, p);
                } else if (relaxedActive) {
                    // Boundary straddler: keep it, route to every sketch it overlaps.
                    addAggregatedDataPointToOverlappingSketches(from, to, timeUnit, sketches, p);
                }
                // else: drop the straddler (strict single-resolution discipline).
            }
        }
    }

    /**
     * Route a (possibly-straddling) aggregate data point to every sketch its time
     * range overlaps. The shared-partial discipline: a sub-bucket that crosses an
     * outer-bucket boundary is added to BOTH adjacent sketches. The chained-sketch
     * seam-dedupe in {@code ApproxOLSSketch.combine()} ensures it appears once in
     * any concatenated sub-bucket list.
     */
    public static void addAggregatedDataPointToOverlappingSketches(long from, long to,
                                                                   AggregateInterval timeUnit,
                                                                   List<Sketch> sketches,
                                                                   AggregatedDataPoint p) {
        long unitMs = timeUnit.toDuration().toMillis();
        long pFrom = Math.max(p.getFrom(), from);
        long pToExclusive = Math.min(p.getTo(), to);
        if (pFrom >= pToExclusive) {
            return;
        }
        int firstIdx = (int) ((pFrom - from) / unitMs);
        // -1 to land in the last sketch whose start ≤ p.getTo() − 1 (right-exclusive).
        int lastIdx = (int) ((pToExclusive - 1 - from) / unitMs);
        if (firstIdx < 0) firstIdx = 0;
        if (lastIdx >= sketches.size()) lastIdx = sketches.size() - 1;
        for (int i = firstIdx; i <= lastIdx; i++) {
            sketches.get(i).addAggregatedDataPoint(p);
        }
    }

    
    public static int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    public static void addAggregatedDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint) {
        int pixelColumnIndex = getPixelColumnForTimestamp(aggregatedDataPoint.getFrom(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth() 
        && !pixelColumns.get(pixelColumnIndex).hasNoError()) {
            pixelColumns.get(pixelColumnIndex).addAggregatedDataPoint(aggregatedDataPoint);
        }
        // Since we only consider spans with intervals smaller than the pixel column interval, we know that the data point will not overlap more than two pixel columns.
        if (pixelColumnIndex <  viewPort.getWidth() - 1 && pixelColumns.get(pixelColumnIndex + 1).overlaps(aggregatedDataPoint) 
            && !pixelColumns.get(pixelColumnIndex + 1).hasNoError()) {
            // If the next pixel column overlaps the data point, then we need to add the data point to the next pixel column as well.
            pixelColumns.get(pixelColumnIndex + 1).addAggregatedDataPoint(aggregatedDataPoint);
        }
    }

    public static void addDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, DataPoint dataPoint){
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint);
        }
    }

    
}
