package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<MeasureAggregationRequest> measureRequests, Set<String> aggregateFunctions) {
        return new InfluxDBAggregatedDatapoints(influxDBQueryExecutor, dataset, from, to, measureRequests, aggregateFunctions);
    }

    public void closeConnection(){
        influxDBQueryExecutor.closeConnection();
    }
}