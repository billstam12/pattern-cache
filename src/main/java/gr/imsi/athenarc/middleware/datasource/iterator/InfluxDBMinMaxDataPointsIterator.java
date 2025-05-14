package gr.imsi.athenarc.middleware.datasource.iterator;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.NonTimestampedStatsAggregator;

import java.util.List;
import java.util.Map;

public class InfluxDBMinMaxDataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private final Map<String, Integer> measuresMap;

    public InfluxDBMinMaxDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        super(tables);
        this.measuresMap = measuresMap;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator();
        String measureName = "";

        for (int i = 0; i < 2 && current < currentSize; i++) {
            FluxRecord record = currentRecords.get(current);
            measureName = record.getField();
            Object value = record.getValue();
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();                
                statsAggregator.accept(doubleValue);
               
            }
            current++;
        }

        updateGroupTimestamps();
        
        if (statsAggregator.getCount() == 0 && hasNext()) {
            return next();
        }

        AggregatedDataPoint point = new ImmutableAggregatedDataPoint(
            startGroupTimestamp, 
            endGroupTimestamp, 
            measuresMap.get(measureName),
            statsAggregator
        );

        logAggregatedPoint(point, statsAggregator);
        startGroupTimestamp = endGroupTimestamp;
        
        return point;
    }

    private void updateGroupTimestamps() {
        if (current == currentSize) {
            endGroupTimestamp = getTimestampFromRecord(currentRecords.get(currentSize - 1), "_stop");
        } else {
            endGroupTimestamp = getTimestampFromRecord(currentRecords.get(current), "_time");
        }
    }

    private void logAggregatedPoint(AggregatedDataPoint point, NonTimestampedStatsAggregator stats) {
        LOG.debug("Created aggregate Datapoint {} - {} first: {}, last {}, min: {}, max: {}, for measure: {}",
            DateTimeUtil.format(point.getFrom()),
            DateTimeUtil.format(point.getTo()),
            stats.getFirstValue(),
            stats.getLastValue(),
            stats.getMinValue(),
            stats.getMaxValue(),
            point.getMeasure());
    }
}