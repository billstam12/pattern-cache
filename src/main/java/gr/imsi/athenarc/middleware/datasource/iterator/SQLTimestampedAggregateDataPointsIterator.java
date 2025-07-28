package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class SQLTimestampedAggregateDataPointsIterator extends SQLIterator<AggregatedDataPoint> {
    
    private final Map<String, Integer> measuresMap;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    public SQLTimestampedAggregateDataPointsIterator(ResultSet resultSet, Map<String, Integer> measuresMap,
                                                    Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        super(resultSet);
        this.measuresMap = measuresMap;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        try {
            // Extract data from current row
            long timeBucket = getTimestampFromResultSet("time_bucket");
            String measureName = getSafeStringValue("measure_name");
            
            if (measureName == null || !measuresMap.containsKey(measureName)) {
                LOG.warn("Invalid measure field in record: {}", measureName);
                if (hasNext()) {
                    return next();
                }
                throw new RuntimeException("No valid data found");
            }

            Integer measureIndex = measuresMap.get(measureName);
            
            // Calculate bucket end from the aggregate interval
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIndex);
            long intervalMillis = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());
            long timeBucketEnd = timeBucket + intervalMillis;
            
            // Create aggregate stats and populate with available values AND timestamps
            StatsAggregator stats = new StatsAggregator();
            
            // Extract aggregate values AND their timestamps
            try {
                Double minValue = getSafeDoubleValue("min");
                Long minTimestamp = getSafeTimestampValue("min_timestamp");
                if (minValue != null) {
                    stats.accept(new ImmutableDataPoint(minTimestamp, minValue, measureIndex));
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double maxValue = getSafeDoubleValue("max");
                Long maxTimestamp = getSafeTimestampValue("max_timestamp");
                if (maxValue != null) {
                    stats.accept(new ImmutableDataPoint(maxTimestamp, maxValue, measureIndex));
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double firstValue = getSafeDoubleValue("first");
                Long firstTimestamp = getSafeTimestampValue("first_timestamp");
                if (firstValue != null) {
                    stats.accept(new ImmutableDataPoint(firstTimestamp, firstValue, measureIndex));
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double lastValue = getSafeDoubleValue("last");
                Long lastTimestamp = getSafeTimestampValue("last_timestamp");
                if (lastValue != null) {
                    stats.accept(new ImmutableDataPoint(lastTimestamp, lastValue, measureIndex));
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            logAggregateDataPoint(timeBucket, timeBucketEnd, measureName, stats);
            return new ImmutableAggregatedDataPoint(timeBucket, timeBucketEnd, measureIndex, stats);

        } catch (SQLException e) {
            LOG.error("Error reading result set", e);
            throw new RuntimeException("Error reading result set", e);
        }
    }

    private Long getSafeTimestampValue(String columnName) throws SQLException {
        try {
            return getTimestampFromResultSet(columnName);
        } catch (SQLException e) {
            return null;
        }
    }

    private void logAggregateDataPoint(long timeBucket, long timeBucketEnd, String measureName, StatsAggregator stats) {
        String firstValue = "N/A";
        String lastValue = "N/A";
        
        try {
            firstValue = String.valueOf(stats.getFirstValue());
        } catch (Exception e) {
            // First value not available
        }
        
        try {
            lastValue = String.valueOf(stats.getLastValue());
        } catch (Exception e) {
            // Last value not available
        }
        
        LOG.debug("Created aggregate DataPoint for measure {} at time bucket {}: min={}, max={}, first={}, last={}",
                measureName, timeBucket, stats.getMinValue(), stats.getMaxValue(), firstValue, lastValue);
    }

    private long getChronoUnitMillis(ChronoUnit unit) {
        switch (unit) {
            case MILLIS:
                return 1;
            case SECONDS:
                return 1000;
            case MINUTES:
                return 60 * 1000;
            case HOURS:
                return 60 * 60 * 1000;
            case DAYS:
                return 24 * 60 * 60 * 1000;
            case WEEKS:
                return 7 * 24 * 60 * 60 * 1000;
            case MONTHS:
                return 30L * 24L * 60L * 60L * 1000L; // Approximate
            case YEARS:
                return 365L * 24L * 60L * 60L * 1000L; // Approximate
            default:
                throw new IllegalArgumentException("Unsupported ChronoUnit: " + unit);
        }
    }
}   
