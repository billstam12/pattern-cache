package gr.imsi.athenarc.middleware.datasource.iterator;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;

public class InfluxDBDataPointsIterator extends InfluxDBIterator<DataPoint> {
    
    private final Map<String, Integer> measuresMap;
    
    public InfluxDBDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        super(tables);
        this.measuresMap = measuresMap;
    }
    
   
    @Override
    protected DataPoint getNext() {
        FluxRecord record = currentRecords.get(current++);
        if (record == null) {
            throw new NoSuchElementException("Invalid record at position " + (current - 1));
        }

        String measure = record.getField();
        if (measure == null || !measuresMap.containsKey(measure)) {
            throw new IllegalStateException("Invalid measure field in record");
        }

        Object value = record.getValue();
        if (!(value instanceof Number)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skipping null or non-numeric value for measure {} at time {}", 
                    measure, DateTimeUtil.format(record.getTime().toEpochMilli()));
            }
            return next();
        }

        long timestamp = getTimestampFromRecord(record, "_time");
        return new ImmutableDataPoint(
            timestamp,
            ((Number) value).doubleValue(),
            measuresMap.get(measure)
        );
    }
}
