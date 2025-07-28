package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregateStats;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class SQLAggregateDataPointsIterator extends SQLIterator<AggregatedDataPoint> {
    
    private final Map<String, Integer> measuresMap;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    public SQLAggregateDataPointsIterator(ResultSet resultSet, Map<String, Integer> measuresMap, 
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
            
            // Create aggregate stats and populate with available values
            AggregateStats stats = new AggregateStats();
            stats.setFrom(timeBucket);
            stats.setTo(timeBucketEnd);

            // Extract aggregate values based on available columns
            try {
                Double minValue = getSafeDoubleValue("min");
                if (minValue != null) {
                    stats.setMinValue(minValue);
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double maxValue = getSafeDoubleValue("max");
                if (maxValue != null) {
                    stats.setMaxValue(maxValue);
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double firstValue = getSafeDoubleValue("first");
                if (firstValue != null) {
                    stats.setFirstValue(firstValue);
                }
            } catch (SQLException e) {
                // Column doesn't exist, skip
            }

            try {
                Double lastValue = getSafeDoubleValue("last");
                if (lastValue != null) {
                    stats.setLastValue(lastValue);
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

    public void logAggregateDataPoint(long timeBucket, long timeBucketEnd, String measureName, AggregateStats stats) {    
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
        
        LOG.debug("Created aggregate DataPoint for measure {}: min={}, max={}, first={}, last={} in bucket [{} - {}]",
            measureName, stats.getMinValue(), stats.getMaxValue(), firstValue, lastValue, timeBucket, timeBucketEnd);
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
