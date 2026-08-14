package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.SqlDialect;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared base for SQL-flavored data sources (Postgres, Trino, DuckDB).
 * Subclasses provide a JDBC executor, dataset metadata, and a {@link SqlDialect}
 * that captures the engine-specific SQL fragments.
 */
public abstract class SqlLikeDatasource implements DataSource {

    protected final SQLQueryExecutor queryExecutor;
    protected final SQLDataset dataset;
    protected final SqlDialect dialect;

    protected SqlLikeDatasource(SQLQueryExecutor queryExecutor, SQLDataset dataset, SqlDialect dialect) {
        this.queryExecutor = queryExecutor;
        this.dataset = dataset;
        this.dialect = dialect;
    }

    @Override
    public AbstractDataset getDataset() {
        return dataset;
    }

    @Override
    public DataPoints getDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new SqlLikeDataPoints(queryExecutor, dialect, dataset, from, to, missingIntervalsPerMeasure);
    }

    @Override
    public boolean supportsRawSampling() {
        return true;
    }

    @Override
    public DataPoints getRawSamples(long from, long to,
            Map<Integer, List<TimeInterval>> intervalsPerMeasure, long bucketMs,
            int fromRankExclusive, int toRankInclusive) {
        return new SqlLikeSampledDataPoints(queryExecutor, dialect, dataset, from, to,
                intervalsPerMeasure, bucketMs, fromRankExclusive, toRankInclusive);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new SqlLikeAggregatedDatapoints(queryExecutor, dialect, dataset, from, to,
                missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPointsWithTimestamps(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new SqlLikeTimestampedAggregatedDatapoints(queryExecutor, dialect, dataset, from, to,
                missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public AggregatedDataPoints getSlopeAggregates(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure, boolean includeMinMax) {
        if (!dialect.supportsSlopeAggregates()) {
            throw new UnsupportedOperationException(
                    "Slope aggregates not supported by this SQL dialect (" + dialect.getClass().getSimpleName() + ")");
        }
        return new SqlLikeSlopeAggregatedDatapoints(queryExecutor, dialect, dataset, from, to,
                missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, includeMinMax);
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    @Override
    public void closeConnection() {
        queryExecutor.closeConnection();
    }
}
