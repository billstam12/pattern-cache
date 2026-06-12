package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLSlopeAggregatedDataPointsIterator;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.SqlDialect;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Bucket-level OLS sums: sum_x, sum_y, sum_xy, sum_x2, count, optionally min and max.
 * Each bucket emits one row whose normalized x is fractional position within the bucket
 * (the bucket index is added back in OLSSketch).
 */
public class SqlLikeSlopeAggregatedDatapoints implements AggregatedDataPoints {

    private final SQLDataset dataset;
    private final SQLQueryExecutor queryExecutor;
    private final SqlDialect dialect;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;
    private final boolean includeMinMax;

    public SqlLikeSlopeAggregatedDatapoints(SQLQueryExecutor queryExecutor, SqlDialect dialect, SQLDataset dataset,
                                            long from, long to,
                                            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
                                            boolean includeMinMax) {
        this.queryExecutor = queryExecutor;
        this.dialect = dialect;
        this.dataset = dataset;
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        this.includeMinMax = includeMinMax;

        if (missingIntervalsPerMeasure == null || missingIntervalsPerMeasure.isEmpty()
                || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.isEmpty()) {
            throw new IllegalArgumentException("No measures specified");
        }
    }

    @Override
    public Iterator<AggregatedDataPoint> iterator() {
        String tableName = dataset.getTableName();
        String[] headers = dataset.getHeader();
        String timestampColumn = dataset.getTimestampColumn();

        Map<String, Integer> measuresMap = new HashMap<>();
        List<String> dataSourceQueries = new ArrayList<>();

        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                dataSourceQueries.add(buildDataSourceQuery(tableName, measureName, timestampColumn, from, to));
            } else {
                for (TimeInterval interval : missingIntervals) {
                    dataSourceQueries.add(buildDataSourceQuery(tableName, measureName, timestampColumn,
                            interval.getFrom(), interval.getTo()));
                }
            }
        }

        long datasetStart = dataset.getTimeRange().getFrom();
        List<String> selectParts = new ArrayList<>();
        int dataSourceCounter = 0;
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            long intervalMs = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());
            // Anchor x at the dataset's aligned-start so normalized_time = bucket_index +
            // fractional_offset (absolute position), matching the MatchRecognize SQL path
            // and letting OLSSketch consume sums without re-adding windowId. Without this
            // anchor the SQL emits only [0,1) fractional and Java has to splice the bucket
            // index back in via count*windowId — same answer numerically but loses the
            // symmetry MatchRecognize relies on and re-creates the cancellation footprint.
            long alignedStart = DateTimeUtil.alignToTimeUnitBoundary(datasetStart, aggregateInterval, true);

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                long offset = from % intervalMs;
                selectParts.add(buildSlopeAggregateQuery(dataSourceQueries.get(dataSourceCounter++),
                        timestampColumn, intervalMs, offset, alignedStart));
            } else {
                for (TimeInterval interval : missingIntervals) {
                    long offset = interval.getFrom() % intervalMs;
                    selectParts.add(buildSlopeAggregateQuery(dataSourceQueries.get(dataSourceCounter++),
                            timestampColumn, intervalMs, offset, alignedStart));
                }
            }
        }

        String sqlQuery = String.join(" UNION ALL ", selectParts) + " ORDER BY measure_name, time_bucket";
        ResultSet resultSet = queryExecutor.executeDbQuery(sqlQuery);
        return new SQLSlopeAggregatedDataPointsIterator(resultSet, measuresMap, aggregateIntervalsPerMeasure);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value as _value, id as _measure " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= " + dialect.unixSecondsToTimestamp(fromTime / 1000.0) + " " +
                "AND " + timestampColumn + " < " + dialect.unixSecondsToTimestamp(toTime / 1000.0) + " " +
                "AND id = '" + measureName + "'";
    }

    private String buildSlopeAggregateQuery(String dataSourceQuery, String timestampColumn,
                                            long intervalMillis, long offset, long alignedStart) {
        String timeBucket = generateTimeBucketExpression(timestampColumn, intervalMillis, offset);
        String unixMsTs = dialect.timestampToUnixMillis(timestampColumn);
        String unixMsBucket = dialect.timestampToUnixMillis("bucket_start");

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
        // normalized_time = bucket_index_from_alignedStart + fractional_offset_in_bucket.
        // The FLOOR term is an exact integer (the bucket index relative to alignedStart);
        // the % term is the within-bucket fractional position in [0,1). Their sum is the
        // absolute x position in interval units, so consumers (OLSSketch) don't have to
        // re-add windowId — same convention as MatchRecognizeQueryExecutor.
        query.append("    FLOOR((").append(unixMsTs).append(" - ").append(alignedStart).append(") / ")
                .append(intervalMillis).append(") + ")
                .append("((").append(unixMsTs).append(" - ").append(unixMsBucket).append(") % ")
                .append(intervalMillis).append(") / ").append(intervalMillis).append(".0 AS normalized_time\n");
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

    private String generateTimeBucketExpression(String timestampColumn, long intervalMillis, long offset) {
        String unixMs = dialect.timestampToUnixMillis(timestampColumn);
        String secondsExpr = "(" + offset + " + FLOOR((" + unixMs + " - " + offset + ") / "
                + intervalMillis + ") * " + intervalMillis + ") / 1000.0";
        return dialect.unixSecondsToTimestamp(secondsExpr);
    }

    private long getChronoUnitMillis(ChronoUnit unit) {
        switch (unit) {
            case MILLIS: return 1;
            case SECONDS: return 1000;
            case MINUTES: return 60_000L;
            case HOURS: return 60L * 60_000L;
            case DAYS: return 24L * 60L * 60_000L;
            case WEEKS: return 7L * 24L * 60L * 60_000L;
            case MONTHS: return 30L * 24L * 60L * 60_000L;
            case YEARS: return 365L * 24L * 60L * 60_000L;
            default: throw new IllegalArgumentException("Unsupported ChronoUnit: " + unit);
        }
    }

    @Override public long getFrom() { return from; }
    @Override public long getTo() { return to; }
    @Override public String getFromDate() { return DateTimeUtil.format(from); }
    @Override public String getToDate() { return DateTimeUtil.format(to); }
    @Override public String getFromDate(String format) { return DateTimeUtil.format(from, format); }
    @Override public String getToDate(String format) { return DateTimeUtil.format(to, format); }
}
