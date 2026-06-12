package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLDataPointsIterator;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.SqlDialect;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.util.*;

public class SqlLikeDataPoints implements DataPoints {

    private final SQLDataset dataset;
    private final SQLQueryExecutor queryExecutor;
    private final SqlDialect dialect;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;

    public SqlLikeDataPoints(SQLQueryExecutor queryExecutor, SqlDialect dialect, SQLDataset dataset, long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.dataset = dataset;
        this.queryExecutor = queryExecutor;
        this.dialect = dialect;

        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.isEmpty()) {
            throw new IllegalArgumentException("No measures specified");
        }
    }

    @Override
    public Iterator<DataPoint> iterator() {
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

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(String.join(" UNION ALL ", dataSourceQueries));
        sqlQuery.append(" ORDER BY ").append(timestampColumn).append(", id");

        ResultSet resultSet = queryExecutor.executeDbQuery(sqlQuery.toString());
        return new SQLDataPointsIterator(resultSet, measuresMap, timestampColumn);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value, id " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= " + dialect.unixSecondsToTimestamp(fromTime / 1000.0) + " " +
                "AND " + timestampColumn + " < " + dialect.unixSecondsToTimestamp(toTime / 1000.0) + " " +
                "AND id = '" + measureName + "'";
    }

    @Override
    public long getFrom() { return from; }

    @Override
    public long getTo() { return to; }

    @Override
    public String getFromDate() { return ""; }

    @Override
    public String getToDate() { return ""; }

    @Override
    public String getFromDate(String format) { return DateTimeUtil.format(from, format); }

    @Override
    public String getToDate(String format) { return DateTimeUtil.format(to, format); }
}
