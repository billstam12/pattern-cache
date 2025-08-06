package gr.imsi.athenarc.middleware.datasource.trino;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TrinoSlopeAggregatedDataPoints implements AggregatedDataPoints {

    public TrinoSlopeAggregatedDataPoints(SQLQueryExecutor trinoQueryExecutor, SQLDataset dataset, long from, long to,
            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        super();
        // Implementation for Trino slope aggregated data points
        // Query: Calculate slopes using LAG/LEAD functions or subqueries with Trino syntax
    }

    @Override
    public Iterator<AggregatedDataPoint> iterator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'iterator'");
    }

    @Override
    public long getFrom() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFrom'");
    }

    @Override
    public long getTo() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTo'");
    }
}
