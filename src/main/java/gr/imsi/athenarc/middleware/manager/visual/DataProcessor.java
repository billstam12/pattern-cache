package gr.imsi.athenarc.middleware.manager.visual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.RawTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.*;

import java.time.temporal.ChronoUnit;
import java.util.*;

public class DataProcessor {

    private final DataSource dataSource;
    private final int dataReductionRatio;

    protected DataProcessor(DataSource dataSource, int dataReductionRatio){
        this.dataSource = dataSource;
        this.dataReductionRatio = dataReductionRatio;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    
    public RangeSet<Long> getRawTimeSeriesSpanRanges(List<TimeSeriesSpan> timeSeriesSpans) {
        RangeSet<Long> rangeSet = TreeRangeSet.create();

        for (TimeSeriesSpan span : timeSeriesSpans) {
            if (span instanceof RawTimeSeriesSpan) {
                long spanFrom = span.getFrom();
                long spanTo = span.getTo();
                rangeSet.add(Range.closed(spanFrom, spanTo)); // closed in order for the enclosed check to work
            }
        }

        return rangeSet;
    }
    

    /**
     * Add a list of timeseriesspans to their respective pixel columns.
     * Each span and pixel column list represents a specific measure.
     * @param from start of query
     * @param to end of query
     * @param viewPort viewport of query
     * @param pixelColumns pixel columns of measure
     * @param timeSeriesSpans time series spans for measure
     */
    protected void processDatapoints(long from, long to, ViewPort viewPort,
                                   List<PixelColumn> pixelColumns, List<TimeSeriesSpan> timeSeriesSpans) {


        // Get the ranges from raw time series spans
        RangeSet<Long> rawSpanRanges = getRawTimeSeriesSpanRanges(timeSeriesSpans);
        LOG.info("Raw span ranges: {}", rawSpanRanges);

        // Mark pixel columns that fall completely within any of the raw span ranges
        for (PixelColumn pixelColumn : pixelColumns) {
            Range<Long> pixelColumnRange = Range.closed(pixelColumn.getFrom(), pixelColumn.getTo());
            if (rawSpanRanges.encloses(pixelColumnRange)) {
                pixelColumn.markAsNoError();
            }
        }

        for (TimeSeriesSpan span : timeSeriesSpans) {
            if (span instanceof RawTimeSeriesSpan) {
                Iterator<DataPoint> iterator = ((RawTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    DataPoint dataPoint = iterator.next();
                    addDataPointToPixelColumns(from, to, viewPort, pixelColumns, dataPoint);
                }
            } else if (span instanceof AggregateTimeSeriesSpan) {
                // Add aggregated data points to pixel columns with errors
                Iterator<AggregatedDataPoint> iterator = ((AggregateTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = iterator.next();
                    addAggregatedDataPointToPixelColumns(from, to, viewPort, pixelColumns, aggregatedDataPoint);
                }
            } else {
                throw new IllegalArgumentException("Time Series Span Read Error");
            }
        }
    }

    protected Map<Integer, List<TimeInterval>> sortMeasuresAndIntervals(Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        // Sort the map by measure alphabetically
        Map<Integer, List<TimeInterval>> sortedMap = new TreeMap<>(Comparator.comparing(Object::toString));
        sortedMap.putAll(missingIntervalsPerMeasure);

        // Sort each list of intervals based on the getFrom() epoch
        for (List<TimeInterval> intervals : sortedMap.values()) {
            if(intervals.size() > 1)
                intervals.sort(Comparator.comparingLong(TimeInterval::getFrom));
        }

        // Update the original map with the sorted values
        missingIntervalsPerMeasure.clear();
        missingIntervalsPerMeasure.putAll(sortedMap);
        return sortedMap;
    }
    /**
     * Get missing data between the range from-to. THe data are fetched for each measure and each measure has a list of missingIntervals as well as
     * an aggregationFactor.
     * @param from start of query
     * @param to end of query
     * @param missingIntervalsPerMeasure missing intervals per measure
     * @param aggFactors aggregation factors per measure
     * @return A list of TimeSeriesSpan for each measure.
     **/
    protected Map<Integer, List<TimeSeriesSpan>> getMissing(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                 Map<Integer, Integer> aggFactors, ViewPort viewPort) {
        missingIntervalsPerMeasure = sortMeasuresAndIntervals(missingIntervalsPerMeasure); // This helps with parsing the query results
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = new HashMap<>(missingIntervalsPerMeasure.size());
        Map<Integer, AggregateInterval> aggregateIntervals = new HashMap<>(missingIntervalsPerMeasure.size());

        long rawAggregateInterval = dataSource.getDataset().getSamplingInterval();  
        boolean fetchRaw = false;
        for (Map.Entry<Integer, List<TimeInterval>> entry : missingIntervalsPerMeasure.entrySet()) {
            int measure = entry.getKey();
            List<TimeInterval> missingIntervals = entry.getValue();
            int aggFactor = aggFactors.get(measure);

            // Find the largest time interval
            TimeInterval largestInterval = null;
            for (TimeInterval interval : missingIntervals) {
                if (largestInterval == null || (interval.getTo() - interval.getFrom()) > (largestInterval.getTo() - largestInterval.getFrom())) {
                    largestInterval = interval;
                }
            }

            // Fetch raw if the largest portion of data to be fetched is close to raw data
            if (largestInterval != null) {
                long intervalFrom = largestInterval.getFrom();
                long intervalTo = largestInterval.getTo();
                int noOfGroups = viewPort.getWidth() * aggFactor;
                long aggInterval = (intervalTo - intervalFrom) / noOfGroups;

                if (aggInterval < dataReductionRatio * rawAggregateInterval) {
                    fetchRaw = true;
                    break;
                }
            }
        }
        if(fetchRaw) {
            DataPoints missingDataPoints = null;
            LOG.info("Fetching missing raw data from data source");
            missingDataPoints = dataSource.getDataPoints(from, to, missingIntervalsPerMeasure);
            LOG.info("Fetched missing raw data from data source");
            timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPoints, missingIntervalsPerMeasure);
        }
        else {
            for(int measure : aggFactors.keySet()) {
                int noOfGroups = aggFactors.get(measure) * viewPort.getWidth();
                long interval = (to - from) / noOfGroups;
                AggregateInterval aggInterval = new AggregateInterval(interval, ChronoUnit.MILLIS);
                aggregateIntervals.put(measure, aggInterval);
            }
            AggregatedDataPoints missingDataPoints = null;
            Set<String> aggregateFunctions = new HashSet<>();
            aggregateFunctions.add("min");
            aggregateFunctions.add("max");

            LOG.info("Fetching missing data from data source");
            missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervalsPerMeasure, aggregateIntervals, aggregateFunctions);
            LOG.info("Fetched missing data from data source");
            timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingIntervalsPerMeasure, aggregateIntervals);
        }
        return timeSeriesSpans;
    }

    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    private void addAggregatedDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint) {
        int pixelColumnIndex = getPixelColumnForTimestamp(aggregatedDataPoint.getFrom(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth() && !pixelColumns.get(pixelColumnIndex).hasNoError()) {
            pixelColumns.get(pixelColumnIndex).addAggregatedDataPoint(aggregatedDataPoint);
        }
        // Since we only consider spans with intervals smaller than the pixel column interval, we know that the data point will not overlap more than two pixel columns.
        if (pixelColumnIndex <  viewPort.getWidth() - 1 && pixelColumns.get(pixelColumnIndex + 1).overlaps(aggregatedDataPoint) 
            && !pixelColumns.get(pixelColumnIndex + 1).hasNoError()) {
            // If the next pixel column overlaps the data point, then we need to add the data point to the next pixel column as well.
            pixelColumns.get(pixelColumnIndex + 1).addAggregatedDataPoint(aggregatedDataPoint);
        }
    }

    private void addDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, DataPoint dataPoint){
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint);
        }
    }
}
