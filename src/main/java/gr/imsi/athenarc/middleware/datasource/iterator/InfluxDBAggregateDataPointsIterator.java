package gr.imsi.athenarc.middleware.datasource.iterator;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregateStats;

import java.util.List;
import java.util.Map;

public class InfluxDBAggregateDataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private final Map<String, Integer> measuresMap;

    private final int noOfAggregates;

    public InfluxDBAggregateDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap, int noOfAggregates) {
        super(tables);
        this.measuresMap = measuresMap;
        this.noOfAggregates = noOfAggregates;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        AggregateStats statsAggregator = new AggregateStats();
        String measureName = "";

        for (int i = 0; i < noOfAggregates && current < currentSize; i++) {
            FluxRecord record = currentRecords.get(current);
            measureName = record.getField();
            Object value = record.getValue();
            String aggType = (String) record.getValues().get("agg");
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();                
                switch (aggType) {
                    case "min":
                        statsAggregator.setMinValue(doubleValue);
                        break;
                    case "max":
                        statsAggregator.setMaxValue(doubleValue);
                        break;
                    case "first":
                        statsAggregator.setFirstValue(doubleValue);
                        break;
                    case "last":
                        statsAggregator.setLastValue(doubleValue);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported aggregation type: " + aggType);
                }
            }
            current++;
        }

        updateGroupTimestamps();
        
        if (statsAggregator.getCount() == 0 && hasNext()) {
            return next();
        }

        statsAggregator.setFrom(startGroupTimestamp);
        statsAggregator.setTo(endGroupTimestamp);

        AggregatedDataPoint point = new ImmutableAggregatedDataPoint(
            startGroupTimestamp, 
            endGroupTimestamp, 
            measuresMap.get(measureName),
            statsAggregator
        );

        // logAggregatedPoint(point, statsAggregator);
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

    private void logAggregatedPoint(AggregatedDataPoint point, AggregateStats stats) {
        LOG.debug("Created aggregate Datapoint {} - {} min: {}, max: {}, for measure: {}",
            DateTimeUtil.format(point.getFrom()),
            DateTimeUtil.format(point.getTo()),
            // stats.getFirstValue(),
            // stats.getLastValue(),
            stats.getMinValue(),
            stats.getMaxValue(),
            point.getMeasure());
    }
}