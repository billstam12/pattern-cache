package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.OLSSlopeMinMaxStats;
import gr.imsi.athenarc.middleware.domain.OLSSlopeStats;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Locale;
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
public class SQLSlopeAggregatedDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private final ResultSet resultSet;
    private final Map<String, Integer> measuresMap;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;
    private final boolean hasMinMaxColumns;
    private Boolean hasNextCached;

    /**
     * Constructor for Trino slope aggregated data point iterator.
     *
     * @param resultSet SQL ResultSet containing aggregated regression statistics
     * @param measuresMap Mapping from measure names to their indices in the dataset
     * @param aggregateIntervalsPerMeasure Per-measure bucket width; used to compute
     *                                     the bucket end so each emitted dp has
     *                                     {@code [from, to) = [bucket_start, bucket_start + intervalMs)}.
     *                                     Consumers (OLSSketch) rely on this to
     *                                     rescale sub-bucket sums into the parent
     *                                     sketch's coordinate system.
     */
    public SQLSlopeAggregatedDataPointsIterator(ResultSet resultSet,
                                                Map<String, Integer> measuresMap,
                                                Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        this.resultSet = resultSet;
        this.measuresMap = measuresMap;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        this.hasNextCached = null;

        // Resolve min/max column presence once from the result-set metadata so the
        // per-row read path never probes for absent columns via a caught SQLException.
        boolean minMax = false;
        try {
            ResultSetMetaData md = resultSet.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String label = md.getColumnLabel(i).toLowerCase(Locale.ROOT);
                if (label.equals("min_value") || label.equals("max_value")) {
                    minMax = true;
                    break;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read result set metadata", e);
        }
        this.hasMinMaxColumns = minMax;
    }

    @Override
    public boolean hasNext() {
        if (hasNextCached == null) {
            try {
                hasNextCached = resultSet.next();
            } catch (SQLException e) {
                hasNextCached = false;
                throw new RuntimeException("Error checking for next result", e);
            }
        }
        return hasNextCached;
    }

    @Override
    public AggregatedDataPoint next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more aggregated data points");
        }

        try {
            // Extract data from ResultSet. Route through DateTimeUtil.toEpochMilliUtc
            // so the bucket-start ms is computed under UTC semantics regardless of the
            // JVM's local TZ — matches what the SQL produces.
            long timeBucket = DateTimeUtil.toEpochMilliUtc(resultSet.getTimestamp("time_bucket"));
            String measureName = resultSet.getString("measure_name");

            // Extract regression statistics
            // Handle potential null values with defaults that won't break calculations
            double sumX = getDoubleOrDefault(resultSet, "sum_x", 0.0);
            double sumY = getDoubleOrDefault(resultSet, "sum_y", 0.0);
            double sumXY = getDoubleOrDefault(resultSet, "sum_xy", 0.0);
            double sumX2 = getDoubleOrDefault(resultSet, "sum_x2", 0.0);
            long count = getLongOrDefault(resultSet, "count", 0L);

            // Extract min/max values when those columns are present (resolved once
            // from metadata in the constructor).
            double minValue = Double.NaN;
            double maxValue = Double.NaN;
            if (hasMinMaxColumns) {
                minValue = getDoubleOrDefault(resultSet, "min_value", Double.NaN);
                maxValue = getDoubleOrDefault(resultSet, "max_value", Double.NaN);
            }
            boolean hasMinMax = !(Double.isNaN(minValue) && Double.isNaN(maxValue));

            // Get measure index for the aggregated data point
            Integer measureIdx = measuresMap.get(measureName);
            if (measureIdx == null) {
                throw new IllegalStateException("Unknown measure: " + measureName);
            }

            hasNextCached = null;

            // Bracket the bucket: dp.getTo() = bucket_start + intervalMs. OLSSketch
            // uses (dp.from - sketch.from)/sketchInterval and dp width/sketchInterval
            // to rescale sub-bucket sums into the parent sketch's coordinate system,
            // so a zero-width dp would zero out sumX/sumXY/sumX2 there. Mirrors what
            // SQLAggregateDataPointsIterator already does for MinMax.
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            long intervalMs = aggregateInterval.getMultiplier()
                    * getChronoUnitMillis(aggregateInterval.getChronoUnit());
            long timeBucketEnd = timeBucket + intervalMs;

            if (hasMinMax) {
                OLSSlopeMinMaxStats stats =
                    new OLSSlopeMinMaxStats(sumX, sumY, sumXY, sumX2, (int) count, minValue, maxValue);
                return new ImmutableAggregatedDataPoint(timeBucket, timeBucketEnd, measureIdx, stats);
            } else {
                OLSSlopeStats stats = new OLSSlopeStats(sumX, sumY, sumXY, sumX2, (int) count);
                return new ImmutableAggregatedDataPoint(timeBucket, timeBucketEnd, measureIdx, stats);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Error reading aggregated data point", e);
        }
    }

    private long getChronoUnitMillis(ChronoUnit unit) {
        switch (unit) {
            case MILLIS:  return 1;
            case SECONDS: return 1000;
            case MINUTES: return 60_000L;
            case HOURS:   return 60L * 60_000L;
            case DAYS:    return 24L * 60L * 60_000L;
            case WEEKS:   return 7L * 24L * 60L * 60_000L;
            case MONTHS:  return 30L * 24L * 60L * 60_000L; // approximate; matches SqlLikeSlopeAggregatedDatapoints
            case YEARS:   return 365L * 24L * 60L * 60_000L;
            default: throw new IllegalArgumentException("Unsupported ChronoUnit: " + unit);
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
