package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class SQLDataPointsIterator extends SQLIterator<DataPoint> {
    
    private final Map<String, Integer> measuresMap;
    private final String timestampColumnName;

    public SQLDataPointsIterator(ResultSet resultSet, Map<String, Integer> measuresMap, String timestampColumnName) {
        super(resultSet);
        this.measuresMap = measuresMap;
        this.timestampColumnName = timestampColumnName;
    }

    @Override
    protected DataPoint getNext() {
        try {
            // Extract data from current row
            long timestamp = getTimestampFromResultSet(timestampColumnName);
            String measureName = getSafeStringValue("id");
            Double value = getSafeDoubleValue("value");
            
            if (measureName == null || !measuresMap.containsKey(measureName)) {
                LOG.warn("Invalid measure field in record: {}", measureName);
                if (hasNext()) {
                    return next();
                }
                throw new RuntimeException("No valid data found");
            }

            if (value == null) {
                LOG.debug("Skipping null value for measure {} at timestamp {}", measureName, timestamp);
                if (hasNext()) {
                    return next();
                }
                throw new RuntimeException("No valid data found");
            }

            Integer measureIndex = measuresMap.get(measureName);
            
            LOG.debug("Created DataPoint for measure {} at timestamp {}: value={}", 
                measureName, timestamp, value);

            return new ImmutableDataPoint(timestamp, value, measureIndex);

        } catch (SQLException e) {
            LOG.error("Error reading result set", e);
            throw new RuntimeException("Error reading result set", e);
        }
    }
}
