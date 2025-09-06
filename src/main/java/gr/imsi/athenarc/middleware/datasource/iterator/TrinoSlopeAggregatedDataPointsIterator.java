package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.OLSSlopeStats;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Trino-specific iterator for slope aggregated data points.
 * 
 * This iterator processes ResultSets from Trino queries that calculate
 * linear regression statistics (sum_x, sum_y, sum_xy, sum_x2, count) for
 * time-windowed data. The statistics are compatible with:
 * 1. OLS regression calculations for slope/angle determination
 * 2. MATCH_RECOGNIZE queries in Trino (which support pattern matching)
 * 3. Statistical sketching and caching operations
 * 
 * The iterator converts raw SQL results into OLSSlopeStats objects that
 * encapsulate the five key regression parameters needed for least squares
 * slope calculations.
 */
public class TrinoSlopeAggregatedDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private final ResultSet resultSet;
    private final Map<String, Integer> measuresMap;
    private boolean hasNext;
    private boolean checkedNext;

    /**
     * Constructor for Trino slope aggregated data point iterator.
     * 
     * @param resultSet SQL ResultSet containing aggregated regression statistics
     * @param measuresMap Mapping from measure names to their indices in the dataset
     */
    public TrinoSlopeAggregatedDataPointsIterator(ResultSet resultSet, Map<String, Integer> measuresMap) {
        this.resultSet = resultSet;
        this.measuresMap = measuresMap;
        this.hasNext = true;
        this.checkedNext = false;
    }

    @Override
    public boolean hasNext() {
        if (!checkedNext) {
            try {
                hasNext = resultSet.next();
                checkedNext = true;
            } catch (SQLException e) {
                hasNext = false;
                throw new RuntimeException("Error checking for next result", e);
            }
        }
        return hasNext;
    }

    @Override
    public AggregatedDataPoint next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more aggregated data points");
        }

        try {
            // Extract data from ResultSet
            long timeBucket = resultSet.getTimestamp("time_bucket").getTime();
            String measureName = resultSet.getString("measure_name");

            // Extract regression statistics
            // Handle potential null values with defaults that won't break calculations
            double sumX = getDoubleOrDefault(resultSet, "sum_x", 0.0);
            double sumY = getDoubleOrDefault(resultSet, "sum_y", 0.0);
            double sumXY = getDoubleOrDefault(resultSet, "sum_xy", 0.0);
            double sumX2 = getDoubleOrDefault(resultSet, "sum_x2", 0.0);
            long count = getLongOrDefault(resultSet, "count", 0L);

            // Create OLS slope statistics object
            OLSSlopeStats stats = new OLSSlopeStats(sumX, sumY, sumXY, sumX2, (int) count);

            // Get measure index for the aggregated data point
            Integer measureIdx = measuresMap.get(measureName);
            if (measureIdx == null) {
                throw new IllegalStateException("Unknown measure: " + measureName);
            }

            // Reset the checked flag for next iteration
            checkedNext = false;

            // Return aggregated data point with OLS statistics as the value
            // Use timeBucket as both from and to since it represents a single time bucket
            return new ImmutableAggregatedDataPoint(timeBucket, timeBucket, measureIdx, stats);

        } catch (SQLException e) {
            throw new RuntimeException("Error reading aggregated data point", e);
        }
    }

    /**
     * Safely extract double value from ResultSet, returning default if null.
     */
    private double getDoubleOrDefault(ResultSet rs, String columnName, double defaultValue) throws SQLException {
        double value = rs.getDouble(columnName);
        return rs.wasNull() ? defaultValue : value;
    }

    /**
     * Safely extract long value from ResultSet, returning default if null.
     */
    private long getLongOrDefault(ResultSet rs, String columnName, long defaultValue) throws SQLException {
        long value = rs.getLong(columnName);
        return rs.wasNull() ? defaultValue : value;
    }

    /**
     * Clean up resources when iteration is complete.
     */
    public void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // Log warning but don't throw - this is cleanup
                System.err.println("Warning: Error closing ResultSet: " + e.getMessage());
            }
        }
    }
}
