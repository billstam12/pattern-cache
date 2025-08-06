package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLTimestampedAggregateDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class SQLTimestampedAggregatedDatapoints implements AggregatedDataPoints {

    private static final Set<String> SUPPORTED_AGGREGATE_FUNCTIONS = new HashSet<>(
        Arrays.asList("first", "last", "min", "max")
    );

    private SQLDataset dataset;
    private SQLQueryExecutor sqlQueryExecutor;
    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;
    private Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;
    private Set<String> aggregateFunctions;

    public SQLTimestampedAggregatedDatapoints(SQLQueryExecutor sqlQueryExecutor, SQLDataset dataset, long from, long to,
                                   Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                   Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
                                   Set<String> aggregateFunctions) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        this.dataset = dataset;
        this.sqlQueryExecutor = sqlQueryExecutor;

        if (aggregateFunctions == null || aggregateFunctions.isEmpty()) {
            throw new IllegalArgumentException("No aggregate functions specified");
        }
        for (String function : aggregateFunctions) {
            if (!SUPPORTED_AGGREGATE_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException("Unsupported aggregate function: " + function +
                        ". Supported functions are: " + SUPPORTED_AGGREGATE_FUNCTIONS);
            }
        }
        this.aggregateFunctions = aggregateFunctions;

        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.size() == 0
                || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.size() == 0) {
            throw new IllegalArgumentException("No measures specified");
        }
    }

    @Override
    public Iterator<AggregatedDataPoint> iterator() {
        String tableName = dataset.getTableName();
        String[] headers = dataset.getHeader();
        String timestampColumn = dataset.getTimestampColumn();

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
                String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn, from, to);
                dataSourceQueries.add(dataQuery);
                dataSourceCounter++;
            } else {
                for (TimeInterval interval : missingIntervals) {
                    String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn,
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

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                long offset = from % (aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                String aggQuery = buildTimestampedAggregateQuery(dataSourceQueries.get(dataSourceCounter),
                        timestampColumn, aggregateFunctions, aggregateInterval, offset);
                selectParts.add(aggQuery);
                dataSourceCounter++;
            } else {
                for (TimeInterval interval : missingIntervals) {
                    long offset = interval.getFrom() % (aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                    String aggQuery = buildTimestampedAggregateQuery(dataSourceQueries.get(dataSourceCounter),
                            timestampColumn, aggregateFunctions, aggregateInterval, offset);
                    selectParts.add(aggQuery);
                    dataSourceCounter++;
                }
            }
        }

        sqlQuery.append(String.join(" UNION ALL ", selectParts));
        sqlQuery.append(" ORDER BY measure_name, time_bucket");

        ResultSet resultSet = sqlQueryExecutor.executeDbQuery(sqlQuery.toString());
        return new SQLTimestampedAggregateDataPointsIterator(resultSet, measuresMap, aggregateIntervalsPerMeasure);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value as _value, id as _measure " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= to_timestamp(" + (fromTime / 1000.0) + ") " +
                "AND " + timestampColumn + " < to_timestamp(" + (toTime / 1000.0) + ") " +
                "AND id = '" + measureName + "'";
    }

    /**
     * Like buildAggregateQuery but also fetches timestamps for each stat value.
     * Handles sub-second intervals with millisecond precision to avoid floating-point precision issues.
     */
    private String buildTimestampedAggregateQuery(
            String dataSourceQuery, String timestampColumn,
            Set<String> aggFunctions, AggregateInterval aggregateInterval, long offset
    ) {
        long intervalMillis = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());
        
        // Generate time bucket expression with appropriate precision
        String timeBucket = generateTimeBucketExpression(timestampColumn, intervalMillis, offset);

        // Build inner select with window functions for values AND timestamps
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

        // Build outer select: aggregate values AND get timestamps for min/max
        List<String> outerSelect = new ArrayList<>();
        outerSelect.add("time_bucket");
        outerSelect.add("measure_name");
        
        if (aggFunctions.contains("min")) {
            outerSelect.add("min(_value) AS min");
            outerSelect.add("(ARRAY_AGG(" + timestampColumn + " ORDER BY _value))[1] AS min_timestamp");
        }
        if (aggFunctions.contains("max")) {
            outerSelect.add("max(_value) AS max");
            outerSelect.add("(ARRAY_AGG(" + timestampColumn + " ORDER BY _value DESC))[1] AS max_timestamp");
        }
        if (aggFunctions.contains("first")) {
            outerSelect.add("MIN(first_value) AS first");
            outerSelect.add("MIN(first_timestamp) AS first_timestamp");
        }
        if (aggFunctions.contains("last")) {
            outerSelect.add("MIN(last_value) AS last");
            outerSelect.add("MIN(last_timestamp) AS last_timestamp");
        }

        String query = "SELECT " + String.join(", ", outerSelect) +
                " FROM (" + subquery + ") sub " +
                "GROUP BY time_bucket, measure_name";

        return "(" + query + ")";
    }
    
    private String generateTimeBucketExpression(String timestampColumn, long intervalMillis, long offset) {
        // Always work in milliseconds for precision, convert to seconds only for to_timestamp()
        return "to_timestamp((" + offset + " + FLOOR((EXTRACT(EPOCH FROM " + timestampColumn + ") * 1000 - " +
                offset + ") / " + intervalMillis + ") * " + intervalMillis + ") / 1000.0)";
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
        return "";
    }

    @Override
    public String getToDate() {
        return "";
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
