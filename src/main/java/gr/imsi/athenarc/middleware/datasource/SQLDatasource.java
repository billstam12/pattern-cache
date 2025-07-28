package gr.imsi.athenarc.middleware.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.domain.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLDatasource implements DataSource {

    SQLQueryExecutor sqlQueryExecutor;
    SQLDataset dataset;
    private static final Logger LOG = LoggerFactory.getLogger(SQLDatasource.class);

    public SQLDatasource(SQLQueryExecutor sqlQueryExecutor, SQLDataset dataset) {
        this.dataset = dataset;
        this.sqlQueryExecutor = sqlQueryExecutor;
    }

    @Override
    public AbstractDataset getDataset() {
        return dataset;
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPointsWithTimestamps(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new SQLTimestampedAggregatedDatapoints(sqlQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new SQLAggregatedDatapoints(sqlQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public DataPoints getDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new SQLDataPoints(sqlQueryExecutor, dataset, from, to, missingIntervalsPerMeasure);
    }

    @Override
    public AggregatedDataPoints getSlopeAggregates(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        return new SQLSlopeAggregatedDataPoints(sqlQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
    }

    public void closeConnection(){
        sqlQueryExecutor.closeConnection();
    }
}
