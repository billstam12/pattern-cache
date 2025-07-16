package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.util.*;

public class TimeSeriesSpanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpanFactory.class);

    /**
     * Read from iterators and create raw time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * The raw time series span needs to first collect the raw datapoints and then be built. This is because the sampling interval may vary.
     * @param dataPoints fetched raw datapoints
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @return A list of RawTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createRaw(DataPoints dataPoints, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure){
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<DataPoint> it = dataPoints.iterator();
        DataPoint dataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false;
            for(TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan(range.getFrom(), range.getTo(), measure);
                List<DataPoint> dataPointsList = new ArrayList<>();
                while (true) {
                    // Get next point if needed
                    if (!changed && dataPoint == null && it.hasNext()) {
                        dataPoint = it.next();
                    }
                    
                    // If there's no point to process, break the loop
                    if (dataPoint == null) {
                        break;
                    }
                    
                    if (dataPoint.getTimestamp() < range.getFrom() 
                        || dataPoint.getTimestamp() >= range.getTo()
                        || dataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        LOG.debug("Adding {} between {}-{}", dataPoint.getTimestamp(), range.getFrom(), range.getTo());
                        dataPointsList.add(dataPoint);
                        // Clear current point and get next one in next iteration
                        dataPoint = null;
                    }   
                }
                timeSeriesSpan.build(dataPointsList);
                timeSeriesSpansForMeasure.add(timeSeriesSpan);
                LOG.info("Created raw time series span: {}", timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        return spans;
    }

    /**
     * Read from iterators and create time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggregated data points
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @param aggregateIntervalsPerMeasure aggregate intervals with which to fetch data for each measure
     * @return A list of AggregateTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createM4InfAggregate(AggregatedDataPoints aggregatedDataPoints,
                                                                     Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                     Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        AggregatedDataPoint aggregatedDataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval  aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false; 
            
            for (TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                int count = 0;
                M4InfAggregateTimeSeriesSpan timeSeriesSpan = new M4InfAggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                while (true) {
                    // Get next point if needed
                    if (!changed && aggregatedDataPoint == null && it.hasNext()) {
                        aggregatedDataPoint = it.next();
                    }
                    
                    // If there's no point to process, break the loop
                    if (aggregatedDataPoint == null) {
                        break;
                    }
                
                    if (aggregatedDataPoint.getTimestamp() < range.getFrom()
                            || aggregatedDataPoint.getTimestamp() >= range.getTo() 
                            || aggregatedDataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {}",
                                aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, measure);
                        timeSeriesSpan.addAggregatedDataPoint(aggregatedDataPoint);
                        count ++;
                        // Clear current point and get next one in next iteration
                        aggregatedDataPoint = null;
                    }
                }
                LOG.info("Created aggregate time series span for measure {} : {}/{}", measure, count, timeSeriesSpan.getSize());

                timeSeriesSpansForMeasure.add(timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        
        return spans;
    }


     /**
     * Read from iterators and create time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggregated data points
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @param aggregateIntervalsPerMeasure aggregate intervals with which to fetch data for each measure
     * @return A list of AggregateTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createMinMaxAggregate(AggregatedDataPoints aggregatedDataPoints,
                                                                     Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                     Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        AggregatedDataPoint aggregatedDataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval  aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false; 
            
            for (TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                int count = 0;
                MinMaxAggregateTimeSeriesSpan timeSeriesSpan = new MinMaxAggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                while (true) {
                    // Get next point if needed
                    if (!changed && aggregatedDataPoint == null && it.hasNext()) {
                        aggregatedDataPoint = it.next();
                    }
                    
                    // If there's no point to process, break the loop
                    if (aggregatedDataPoint == null) {
                        break;
                    }
                
                    if (aggregatedDataPoint.getTimestamp() < range.getFrom()
                            || aggregatedDataPoint.getTimestamp() >= range.getTo() 
                            || aggregatedDataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {}",
                                aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, measure);
                        timeSeriesSpan.addAggregatedDataPoint(aggregatedDataPoint);
                        count ++;
                        // Clear current point and get next one in next iteration
                        aggregatedDataPoint = null;
                    }
                }
                LOG.info("Created aggregate time series span for measure {} : {}/{}", measure, count, timeSeriesSpan.getSize());

                timeSeriesSpansForMeasure.add(timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        
        return spans;
    }


    /**
     * Read from iterators and create time series spans that also hold the fetched timestamps.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggregated data points
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @param aggregateIntervalsPerMeasure aggregate intervals with which to fetch data for each measure
     * @return A list of M4AggregateTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createM4Aggregate(AggregatedDataPoints aggregatedDataPoints,
                                                                     Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                     Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        AggregatedDataPoint aggregatedDataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval  aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false; 
            
            for (TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                M4AggregateTimeSeriesSpan timeSeriesSpan = new M4AggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                while (true) {
                    // Get next point if needed
                    if (!changed && aggregatedDataPoint == null && it.hasNext()) {
                        aggregatedDataPoint = it.next();
                    }
                    
                    // If there's no point to process, break the loop
                    if (aggregatedDataPoint == null) {
                        break;
                    }
                
                    if (aggregatedDataPoint.getTimestamp() < range.getFrom()
                            || aggregatedDataPoint.getTimestamp() >= range.getTo() 
                            || aggregatedDataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {}",
                                aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, measure);
                        timeSeriesSpan.addAggregatedDataPoint(aggregatedDataPoint);
                        // Clear current point and get next one in next iteration
                        aggregatedDataPoint = null;
                    }
                }
                LOG.info("Created aggregate time series span for measure {} : {}", measure, timeSeriesSpan.getSize());

                timeSeriesSpansForMeasure.add(timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        
        return spans;
    }

    public static Map<Integer, List<TimeSeriesSpan>> createSlopeAggregate(AggregatedDataPoints slopeAggregatedDataPoints,
                                                                    Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                    Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
    Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = slopeAggregatedDataPoints.iterator();
        AggregatedDataPoint slopeAggregatedDataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval  aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false; 
            
            for (TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                SlopeAggregateTimeSeriesSpan timeSeriesSpan = new SlopeAggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                while (true) {
                    // Get next point if needed
                    if (!changed && slopeAggregatedDataPoint == null && it.hasNext()) {
                        slopeAggregatedDataPoint = it.next();
                    }
                    
                    // If there's no point to process, break the loop
                    if (slopeAggregatedDataPoint == null) {
                        break;
                    }
                
                    if (slopeAggregatedDataPoint.getTimestamp() < range.getFrom()
                            || slopeAggregatedDataPoint.getTimestamp() >= range.getTo() 
                            || slopeAggregatedDataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {}",
                                slopeAggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, measure);
                        timeSeriesSpan.addAggregatedDataPoint(slopeAggregatedDataPoint);
                        // Clear current point and get next one in next iteration
                        slopeAggregatedDataPoint = null;
                    }
                }
                LOG.debug("Created aggregate time series span for measure {} : {}", measure, timeSeriesSpan.getSize());

                timeSeriesSpansForMeasure.add(timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        
        return spans;
    }

}


