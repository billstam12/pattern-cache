package gr.imsi.athenarc.middleware.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.middleware.domain.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfluxDBDatasource implements DataSource {

    InfluxDBQueryExecutor influxDBQueryExecutor;
    InfluxDBDataset dataset;
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDatasource.class);

    public InfluxDBDatasource(InfluxDBQueryExecutor influxDBQueryExecutor, InfluxDBDataset dataset) {
        this.dataset = dataset;
        this.influxDBQueryExecutor = influxDBQueryExecutor;
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
        return new InfluxDBM4Datapoints(influxDBQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure,
            Set<String> aggregateFunctions) {
        return new InfluxDBAggregatedDatapoints(influxDBQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure, aggregateFunctions);
    }

    @Override
    public DataPoints getDataPoints(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new InfluxDBDataPoints(influxDBQueryExecutor, dataset, from, to, missingIntervalsPerMeasure);
    }

   

    @Override
    public AggregatedDataPoints getSlopeAggregates(long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        return new InfluxDBSlopeAggregatedDataPoints(influxDBQueryExecutor, dataset, from, to, missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
    }

    public void closeConnection(){
        influxDBQueryExecutor.closeConnection();
    }
}