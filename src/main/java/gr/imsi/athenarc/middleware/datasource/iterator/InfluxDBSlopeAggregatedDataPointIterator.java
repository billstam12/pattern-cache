package gr.imsi.athenarc.middleware.datasource.iterator;

import java.util.List;
import java.util.Map;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.SlopeStats;

public class InfluxDBSlopeAggregatedDataPointIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private final Map<String, Integer> measuresMap;

    public InfluxDBSlopeAggregatedDataPointIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        super(tables);
        this.measuresMap = measuresMap;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        FluxRecord record = currentRecords.get(current++);
        if (record == null) {
            LOG.warn("Null record encountered, skipping to next");
            return next();
        }

        String measureName = record.getField();
        if (measureName == null || !measuresMap.containsKey(measureName)) {
            LOG.warn("Invalid measure field in record: {}", measureName);
            return next();
        }

        final int measureIndex = measuresMap.get(measureName);
        
        // Extract slope components from the record
        final Double sumX = getSafeDoubleValue(record, "sum_x");
        final Double sumY = getSafeDoubleValue(record, "sum_y");
        final Double sumXY = getSafeDoubleValue(record, "sum_xy");
        final Double sumX2 = getSafeDoubleValue(record, "sum_x2");
        final Integer count = getSafeIntValue(record, "count");
        LOG.info("Start group timestamp: {}, End group timestamp: {}, Measure index: {}, Sum X: {}, Sum Y: {}, Sum XY: {}, Sum X2: {}, Count: {}",
                startGroupTimestamp, endGroupTimestamp, measureIndex, sumX, sumY, sumXY, sumX2, count);
        SlopeStats slopeStats = new SlopeStats(sumX, sumY, sumXY, sumX2, count);
        return new ImmutableAggregatedDataPoint(
                startGroupTimestamp,
                endGroupTimestamp, 
                measureIndex,
                slopeStats);
    }
    
    private Double getSafeDoubleValue(FluxRecord record, String key) {
        Object value = record.getValueByKey(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }
    
    private Integer getSafeIntValue(FluxRecord record, String key) {
        Object value = record.getValueByKey(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }
}
