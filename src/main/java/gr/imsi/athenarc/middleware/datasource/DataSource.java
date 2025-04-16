package gr.imsi.athenarc.middleware.datasource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

/**
 * Represents a time series data source
 */
public interface DataSource {


    /* Returns raw datapoints for each measure */
    DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure);

    /**
     * Returns an {@link AggregatedDataPoints} instance to access aggregated data points for multiple measures,
     * each with its own missing intervals and aggregate interval.
     * @param from global start timestamp
     * @param to global end timestamp
     * @param measureRequests list of measure-specific aggregation requests
     * @param aggregateFunctions the set of aggregate functions to apply (same for all measures)
     * @return aggregated data points
     */
    AggregatedDataPoints getAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, 
                                                    Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure, Set<String> aggregateFunctions);

    public AbstractDataset getDataset();

    public void closeConnection();

}
