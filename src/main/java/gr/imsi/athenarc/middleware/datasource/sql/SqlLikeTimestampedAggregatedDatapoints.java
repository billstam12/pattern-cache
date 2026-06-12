package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLTimestampedAggregateDataPointsIterator;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.SqlDialect;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class SqlLikeTimestampedAggregatedDatapoints implements AggregatedDataPoints {

    private static final Set<String> SUPPORTED_AGGREGATE_FUNCTIONS =
            new HashSet<>(Arrays.asList("first", "last", "min", "max", "count"));

    private final SQLDataset dataset;
    private final SQLQueryExecutor queryExecutor;
    private final SqlDialect dialect;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;
    private final Set<String> aggregateFunctions;

    public SqlLikeTimestampedAggregatedDatapoints(SQLQueryExecutor queryExecutor, SqlDialect dialect, SQLDataset dataset,
                                                  long from, long to,
                                                  Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                  Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
                                                  Set<String> aggregateFunctions) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        this.dataset = dataset;
        this.queryExecutor = queryExecutor;
        this.dialect = dialect;

        if (aggregateFunctions == null || aggregateFunctions.isEmpty()) {
            throw new IllegalArgumentException("No aggregate functions specified");
        }
        for (String fn : aggregateFunctions) {
            if (!SUPPORTED_AGGREGATE_FUNCTIONS.contains(fn)) {
                throw new IllegalArgumentException("Unsupported aggregate function: " + fn
                        + ". Supported: " + SUPPORTED_AGGREGATE_FUNCTIONS);
            }
        }
        this.aggregateFunctions = aggregateFunctions;

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

        List<String> selectParts = new ArrayList<>();
        int dataSourceCounter = 0;
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            long intervalMs = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                long offset = from % intervalMs;
                selectParts.add(buildTimestampedAggregateQuery(dataSourceQueries.get(dataSourceCounter++),
                        timestampColumn, aggregateFunctions, intervalMs, offset));
            } else {
                for (TimeInterval interval : missingIntervals) {
                    long offset = interval.getFrom() % intervalMs;
                    selectParts.add(buildTimestampedAggregateQuery(dataSourceQueries.get(dataSourceCounter++),
                            timestampColumn, aggregateFunctions, intervalMs, offset));
                }
            }
        }

        String sqlQuery = String.join(" UNION ALL ", selectParts) + " ORDER BY measure_name, time_bucket";
        ResultSet resultSet = queryExecutor.executeDbQuery(sqlQuery);
        return new SQLTimestampedAggregateDataPointsIterator(resultSet, measuresMap, aggregateIntervalsPerMeasure);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value as _value, id as _measure " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= " + dialect.unixSecondsToTimestamp(fromTime / 1000.0) + " " +
                "AND " + timestampColumn + " < " + dialect.unixSecondsToTimestamp(toTime / 1000.0) + " " +
                "AND id = '" + measureName + "'";
    }

    private String buildTimestampedAggregateQuery(String dataSourceQuery, String timestampColumn,
                                                  Set<String> aggFunctions, long intervalMillis, long offset) {
        String timeBucket = generateTimeBucketExpression(timestampColumn, intervalMillis, offset);

        List<String> innerSelect = new ArrayList<>();
        innerSelect.add(timeBucket + " AS time_bucket");
        innerSelect.add("_measure AS measure_name");
        innerSelect.add("_value");
        innerSelect.add(timestampColumn);
        if (aggFunctions.contains("first")) {
            innerSelect.add("first_value(_value) OVER w AS first_value");
            innerSelect.add("first_value(" + timestampColumn + ") OVER w AS first_timestamp");
        }
        if (aggFunctions.contains("last")) {
            innerSelect.add("last_value(_value) OVER w AS last_value");
            innerSelect.add("last_value(" + timestampColumn + ") OVER w AS last_timestamp");
        }

        String windowClause = "WINDOW w AS (PARTITION BY " + timeBucket + ", _measure ORDER BY " + timestampColumn +
                " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";

        String subquery = "SELECT " + String.join(", ", innerSelect) +
                " FROM (" + dataSourceQuery + ") data " +
                ((aggFunctions.contains("first") || aggFunctions.contains("last")) ? windowClause : "");

        List<String> outerSelect = new ArrayList<>();
        outerSelect.add("time_bucket");
        outerSelect.add("measure_name");
        if (aggFunctions.contains("min")) {
            outerSelect.add("min(_value) AS min");
            outerSelect.add(dialect.firstOfArrayAgg(timestampColumn, "_value", false) + " AS min_timestamp");
        }
        if (aggFunctions.contains("max")) {
            outerSelect.add("max(_value) AS max");
            outerSelect.add(dialect.firstOfArrayAgg(timestampColumn, "_value", true) + " AS max_timestamp");
        }
        if (aggFunctions.contains("count")) {
            outerSelect.add("count(_value) AS count");
        }
        if (aggFunctions.contains("first")) {
            outerSelect.add(dialect.anyValue("first_value") + " AS first");
            outerSelect.add(dialect.anyValue("first_timestamp") + " AS first_timestamp");
        }
        if (aggFunctions.contains("last")) {
            outerSelect.add(dialect.anyValue("last_value") + " AS last");
            outerSelect.add(dialect.anyValue("last_timestamp") + " AS last_timestamp");
        }

        return "(SELECT " + String.join(", ", outerSelect) +
                " FROM (" + subquery + ") sub " +
                "GROUP BY time_bucket, measure_name)";
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
    @Override public String getFromDate() { return ""; }
    @Override public String getToDate() { return ""; }
    @Override public String getFromDate(String format) { return DateTimeUtil.format(from, format); }
    @Override public String getToDate(String format) { return DateTimeUtil.format(to, format); }
}
