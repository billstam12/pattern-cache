package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.SQLDataPointsIterator;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.SqlDialect;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Returns the raw points whose per-bucket rank lies in {@code (fromRankExclusive,
 * toRankInclusive]}, per measure. Rank is assigned by a deterministic hash of the
 * timestamp within each {@code bucketMs}-wide bucket, so ranks are stable across
 * calls: fetching {@code (0, k]} then {@code (k, 2k]} yields two disjoint samples
 * whose union is the {@code (0, 2k]} sample. The band filter is applied in the
 * database, so only the requested rows cross the wire.
 */
public class SqlLikeSampledDataPoints implements DataPoints {

    private static final long HASH_MULT = 2654435761L;
    private static final long HASH_MOD = 2147483647L;

    private final SQLDataset dataset;
    private final SQLQueryExecutor queryExecutor;
    private final SqlDialect dialect;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> intervalsPerMeasure;
    private final long bucketMs;
    private final int fromRankExclusive;
    private final int toRankInclusive;

    public SqlLikeSampledDataPoints(SQLQueryExecutor queryExecutor, SqlDialect dialect, SQLDataset dataset,
            long from, long to, Map<Integer, List<TimeInterval>> intervalsPerMeasure,
            long bucketMs, int fromRankExclusive, int toRankInclusive) {
        if (intervalsPerMeasure == null || intervalsPerMeasure.isEmpty()) {
            throw new IllegalArgumentException("No measures specified");
        }
        if (bucketMs <= 0) {
            throw new IllegalArgumentException("bucketMs must be > 0");
        }
        if (toRankInclusive <= fromRankExclusive) {
            throw new IllegalArgumentException("toRankInclusive must be > fromRankExclusive");
        }
        this.queryExecutor = queryExecutor;
        this.dialect = dialect;
        this.dataset = dataset;
        this.from = from;
        this.to = to;
        this.intervalsPerMeasure = intervalsPerMeasure;
        this.bucketMs = bucketMs;
        this.fromRankExclusive = fromRankExclusive;
        this.toRankInclusive = toRankInclusive;
    }

    @Override
    public Iterator<DataPoint> iterator() {
        String tableName = dataset.getTableName();
        String[] headers = dataset.getHeader();
        String timestampColumn = dataset.getTimestampColumn();

        Map<String, Integer> measuresMap = new HashMap<>();
        List<String> dataSourceQueries = new ArrayList<>();

        for (int measureIdx : intervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> intervals = intervalsPerMeasure.get(measureIdx);
            if (intervals == null || intervals.isEmpty()) {
                dataSourceQueries.add(buildDataSourceQuery(tableName, measureName, timestampColumn, from, to));
            } else {
                for (TimeInterval interval : intervals) {
                    dataSourceQueries.add(buildDataSourceQuery(tableName, measureName, timestampColumn,
                            interval.getFrom(), interval.getTo()));
                }
            }
        }

        String unioned = String.join(" UNION ALL ", dataSourceQueries);
        String unixMs = "CAST(" + dialect.timestampToUnixMillis(timestampColumn) + " AS BIGINT)";
        String bucketExpr = "FLOOR((" + unixMs + " - " + from + ") / " + bucketMs + ")";
        String hashExpr = "MOD(MOD(" + unixMs + ", " + HASH_MOD + ") * " + HASH_MULT + ", " + HASH_MOD + ")";

        String sql = "SELECT " + timestampColumn + ", value, id FROM ("
                + "SELECT " + timestampColumn + ", value, id, "
                + "row_number() OVER (PARTITION BY id, " + bucketExpr
                + " ORDER BY " + hashExpr + ", " + unixMs + ") AS rn "
                + "FROM (" + unioned + ") d"
                + ") s WHERE rn > " + fromRankExclusive + " AND rn <= " + toRankInclusive
                + " ORDER BY " + timestampColumn + ", id";

        ResultSet resultSet = queryExecutor.executeDbQuery(sql);
        return new SQLDataPointsIterator(resultSet, measuresMap, timestampColumn);
    }

    private String buildDataSourceQuery(String tableName, String measureName, String timestampColumn,
                                        long fromTime, long toTime) {
        return "SELECT " + timestampColumn + ", value, id "
                + "FROM " + tableName + " "
                + "WHERE " + timestampColumn + " >= " + dialect.unixSecondsToTimestamp(fromTime / 1000.0) + " "
                + "AND " + timestampColumn + " < " + dialect.unixSecondsToTimestamp(toTime / 1000.0) + " "
                + "AND id = '" + measureName + "'";
    }

    @Override
    public long getFrom() { return from; }

    @Override
    public long getTo() { return to; }

    @Override
    public String getFromDate() { return ""; }

    @Override
    public String getToDate() { return ""; }
}
