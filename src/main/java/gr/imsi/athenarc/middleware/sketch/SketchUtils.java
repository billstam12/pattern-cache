package gr.imsi.athenarc.middleware.sketch;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Populate sketches with data from a TimeSeriesSpan.
     */
    public static void populateSketchesFromSpans(List<TimeSeriesSpan> spans, 
                                      List<Sketch> sketches, 
                                      long from, 
                                      long to, 
                                      AggregateInterval timeUnit,
                                      ViewPort viewPort,
                                      String method) {

        for(TimeSeriesSpan span : spans) {
            if (span instanceof RawTimeSeriesSpan) {
                // Skip raw spans as they're not useful for pattern detection
                continue;
            }
            Iterator<AggregatedDataPoint> dataPoints = null;
            dataPoints = span.iterator(from, to);
            populateSketchesFromDataPoints(dataPoints, sketches, from, to, timeUnit);
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
