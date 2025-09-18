package gr.imsi.athenarc.middleware.datasource.trino;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLSlopeAggregatedDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Trino-based slope aggregated data points implementation.
 * 
 * This class calculates linear regression statistics (sum_x, sum_y, sum_xy, sum_x2, count) 
 * for time-windowed data using Trino SQL. These statistics can be used to:
 * 1. Calculate regression slopes/angles for pattern matching
 * 2. Provide input data for MATCH_RECOGNIZE queries 
 * 3. Support OLS-based sketching and caching
 * 
 * The approach mirrors the InfluxDB implementation but uses Trino-specific SQL syntax for:
 * - Time bucketing with to_unixtime/from_unixtime functions
 * - Normalized time coordinates within each bucket (0-1 range)  
 * - Statistical aggregation compatible with least squares regression
 * - Trino-specific INTERVAL syntax
 * 
 * Integration with MatchRecognize:
 * The MatchRecognizeQueryExecutor can leverage these pre-computed statistics
 * instead of calculating them inline, improving query performance for large datasets.
 */
public class TrinoSlopeAggregatedDataPoints implements AggregatedDataPoints {

    private final SQLQueryExecutor trinoQueryExecutor;
    private final SQLDataset dataset;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    private final boolean includeMinMax;

    public TrinoSlopeAggregatedDataPoints(SQLQueryExecutor trinoQueryExecutor, SQLDataset dataset, long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            boolean includeMinMax) {
        this.trinoQueryExecutor = trinoQueryExecutor;
        this.dataset = dataset;
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        this.includeMinMax = includeMinMax;

        // Validate parameters
        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.isEmpty() 
            || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.isEmpty()) {
            throw new IllegalArgumentException("No measures specified");
        }
    }

    @Override
    public Iterator<AggregatedDataPoint> iterator() {
        String tableName = dataset.getTableName();
        String[] headers = dataset.getHeader();
        String timestampColumn = dataset.getTimestampColumn();
        String valueColumn = dataset.getValueColumn();

        StringBuilder sqlQuery = new StringBuilder();

        int dataSourceCounter = 0;
        Map<String, Integer> measuresMap = new HashMap<>();
        List<String> dataSourceQueries = new ArrayList<>();

        // Gather per-measure/interval queries
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn, valueColumn, from, to);
                dataSourceQueries.add(dataQuery);
                dataSourceCounter++;
            } else {
                for (TimeInterval interval : missingIntervals) {
                    String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn, valueColumn,
                            interval.getFrom(), interval.getTo());
                    dataSourceQueries.add(dataQuery);
                    dataSourceCounter++;
                }
            }
        }

        List<String> selectParts = new ArrayList<>();
        dataSourceCounter = 0;
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            long alignedStart = DateTimeUtil.alignToTimeUnitBoundary(dataset.getTimeRange().getFrom(), aggregateInterval, true);

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                long offset = from % (aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                String aggQuery = buildSlopeAggregateQuery(dataSourceQueries.get(dataSourceCounter),
                        timestampColumn, aggregateInterval, offset, alignedStart);
                selectParts.add(aggQuery);
                dataSourceCounter++;
            } else {
                for (TimeInterval interval : missingIntervals) {
                    long offset = interval.getFrom() % (aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                    String aggQuery = buildSlopeAggregateQuery(dataSourceQueries.get(dataSourceCounter),
                            timestampColumn, aggregateInterval, offset, alignedStart);
                    selectParts.add(aggQuery);
                    dataSourceCounter++;
                }
            }
        }

        sqlQuery.append(String.join(" UNION ALL ", selectParts));
        sqlQuery.append(" ORDER BY measure_name, time_bucket");

        ResultSet resultSet = trinoQueryExecutor.executeDbQuery(sqlQuery.toString());
    return new SQLSlopeAggregatedDataPointsIterator(resultSet, measuresMap);
    }

    /**
     * Build a data source query that selects raw data points for a specific measure and time range.
     */
    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn, String valueColumn, long from, long to) {
        // Trino uses from_unixtime instead of to_timestamp and follows the same pattern as other Trino classes
        return "SELECT " + timestampColumn + ", " + valueColumn + " as _value, id as _measure " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= from_unixtime(" + (from / 1000.0) + ") " +
                "AND " + timestampColumn + " < from_unixtime(" + (to / 1000.0) + ") " +
                "AND id = '" + measureName + "'";
    }

    /**
     * Build a slope aggregate query that calculates linear regression statistics (sum_x, sum_y, sum_xy, sum_x2, count)
     * for time windows defined by the aggregate interval. Uses Trino-specific SQL syntax and the same normalization 
     * approach as MatchRecognize.
     */
    private String buildSlopeAggregateQuery(String dataSourceQuery, String timestampColumn,
            AggregateInterval aggregateInterval, long offset, long alignedFrom) {
        
        long intervalMillis = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());
        String timeBucket = generateTimeBucketExpression(timestampColumn, intervalMillis, offset);

        StringBuilder query = new StringBuilder();
        query.append("SELECT \n");
        query.append("  ").append(timeBucket).append(" AS time_bucket,\n");
        query.append("  _measure AS measure_name,\n");
        query.append("  SUM(normalized_time) AS sum_x,\n");
        query.append("  SUM(_value) AS sum_y,\n");
        query.append("  SUM(normalized_time * _value) AS sum_xy,\n");
        query.append("  SUM(normalized_time * normalized_time) AS sum_x2,\n");
        query.append("  COUNT(*) AS count");
        if (includeMinMax) {
            query.append(",\n  MIN(_value) AS min_value,\n  MAX(_value) AS max_value");
        }
        query.append("\n");
        query.append("FROM (\n");
        query.append("  SELECT *,\n");

        // query.append("    -- Normalize timestamp using MatchRecognize approach: bucket_index + fractional_position\n");
        // query.append("    FLOOR((to_unixtime(").append(timestampColumn).append(") * 1000 - ").append(alignedStart).append(") / ").append(intervalMillis).append(") + \n");
        query.append("    -- Normalize timestamp using half of the MatchRecognize approach: fractional_position. The bucket_index will be added dynamically on evaluation\n");
        query.append("    ((to_unixtime(").append(timestampColumn).append(") * 1000 - to_unixtime(bucket_start) * 1000) % ").append(intervalMillis).append(") / ").append(intervalMillis).append(".0 AS normalized_time\n");
       
        query.append("  FROM (\n");
        query.append("    SELECT *,\n");
        query.append("      ").append(timeBucket).append(" AS bucket_start\n");
        query.append("    FROM (").append(dataSourceQuery).append(") data\n");
        query.append("  ) bucketed_data\n");
        query.append(") normalized_data\n");
        query.append("GROUP BY ").append(timeBucket).append(", _measure\n");
        query.append("HAVING COUNT(*) > 0");
        return query.toString();
    }

    /**
     * Generate time bucket expression for Trino SQL.
     * Uses Trino-specific functions for date/time manipulation.
     */
    private String generateTimeBucketExpression(String timestampColumn, long intervalMillis, long offset) {
        if (offset == 0) {
            return String.format(
                "from_unixtime(floor(to_unixtime(%s) * 1000 / %d) * %d / 1000)",
                timestampColumn, intervalMillis, intervalMillis
            );
        } else {
            return String.format(
                "from_unixtime((floor((to_unixtime(%s) * 1000 - %d) / %d) * %d + %d) / 1000)",
                timestampColumn, offset, intervalMillis, intervalMillis, offset
            );
        }
    }

    /**
     * Helper method to convert ChronoUnit to milliseconds
     */
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

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return DateTimeUtil.format(from);
    }

    @Override
    public String getToDate() {
        return DateTimeUtil.format(to);
    }

    @Override
    public String getFromDate(String format) {
        return DateTimeUtil.format(from, format);
    }

    @Override
    public String getToDate(String format) {
        return DateTimeUtil.format(to, format);
    }
}
