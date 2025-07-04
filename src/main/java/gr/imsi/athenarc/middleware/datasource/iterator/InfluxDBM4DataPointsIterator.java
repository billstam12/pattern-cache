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

public class InfluxDBM4DataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private final Map<String, Integer> measuresMap;
    private final int noOfAggregates;

    public InfluxDBM4DataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap, int noOfAggregates) {
        super(tables);
        this.measuresMap = measuresMap;
        this.noOfAggregates = noOfAggregates;

    }

    @Override
    protected AggregatedDataPoint getNext() {
        StatsAggregator statsAggregator = new StatsAggregator();
        String measureName = "";

        for (int i = 0; i < noOfAggregates && current < currentSize; i++) {
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