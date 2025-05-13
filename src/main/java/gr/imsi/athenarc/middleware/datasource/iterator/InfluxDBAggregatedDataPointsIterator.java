package gr.imsi.athenarc.middleware.datasource.iterator;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;

import java.util.List;
import java.util.Map;

public class InfluxDBAggregatedDataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private final int pointsPerAggregate;
    private final Map<String, Integer> measuresMap;

    public InfluxDBAggregatedDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap, int pointsPerAggregate) {
        super(tables);
        this.pointsPerAggregate = pointsPerAggregate;
        this.measuresMap = measuresMap;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        StatsAggregator statsAggregator = new StatsAggregator();
        String measureName = "";

        for (int i = 0; i < pointsPerAggregate && current < currentSize; i++) {
            FluxRecord record = currentRecords.get(current);
            measureName = record.getField();
            Object value = record.getValue();
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                long timestamp = getTimestampFromRecord(record, "_time");
                
                statsAggregator.accept(new ImmutableDataPoint(
                    timestamp, 
                    doubleValue,
                    measuresMap.get(measureName)
                ));
               
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

    private void logAggregatedPoint(AggregatedDataPoint point, StatsAggregator stats) {
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