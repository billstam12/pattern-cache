package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.util.*;

public class SQLDataPoints implements DataPoints {

    private SQLDataset dataset;
    private SQLQueryExecutor sqlQueryExecutor;
    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;

    public SQLDataPoints(SQLQueryExecutor sqlQueryExecutor, SQLDataset dataset, long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.dataset = dataset;
        this.sqlQueryExecutor = sqlQueryExecutor;
        
        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.size() == 0) {
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

        // Gather per-measure/interval queries
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn, from, to);
                dataSourceQueries.add(dataQuery);
            } else {
                for (TimeInterval interval : missingIntervals) {
                    String dataQuery = buildDataSourceQuery(tableName, measureName, timestampColumn,
                            interval.getFrom(), interval.getTo());
                    dataSourceQueries.add(dataQuery);
                }
            }
        }

        // Combine all queries with UNION ALL and order by timestamp, then measure
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(String.join(" UNION ALL ", dataSourceQueries));
        sqlQuery.append(" ORDER BY ").append(timestampColumn).append(", id");

        ResultSet resultSet = sqlQueryExecutor.executeDbQuery(sqlQuery.toString());
        return new SQLDataPointsIterator(resultSet, measuresMap, timestampColumn);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value, id " +
                "FROM " + tableName + " " +
                "WHERE " + timestampColumn + " >= to_timestamp(" + (fromTime / 1000.0) + ") " +
                "AND " + timestampColumn + " < to_timestamp(" + (toTime / 1000.0) + ") " +
                "AND id = '" + measureName + "'";
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
