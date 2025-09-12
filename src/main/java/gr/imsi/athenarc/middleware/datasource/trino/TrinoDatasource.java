package gr.imsi.athenarc.middleware.datasource.trino;

import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.domain.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TrinoDatasource implements DataSource {

    SQLQueryExecutor trinoQueryExecutor;
    SQLDataset dataset;

    public TrinoDatasource(SQLQueryExecutor trinoQueryExecutor, SQLDataset dataset) {
        this.dataset = dataset;
        this.trinoQueryExecutor = trinoQueryExecutor;
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
        return new TrinoTimestampedAggregatedDatapoints(trinoQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new TrinoAggregatedDatapoints(trinoQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public DataPoints getDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new TrinoDataPoints(trinoQueryExecutor, dataset, from, to, missingIntervalsPerMeasure);
    }

    @Override
    public AggregatedDataPoints getSlopeAggregates(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure, boolean includeMinMax) {
        return new TrinoSlopeAggregatedDataPoints(trinoQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, includeMinMax);
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return trinoQueryExecutor;
    }

    public void closeConnection(){
        trinoQueryExecutor.closeConnection();
    }
}
