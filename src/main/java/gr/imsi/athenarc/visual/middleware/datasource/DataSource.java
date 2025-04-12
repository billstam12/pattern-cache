package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.domain.MeasureAggregationRequest;

import java.util.List;
import java.util.Set;

/**
 * Represents a time series data source
 */
public interface DataSource {

    /**
     * Returns an {@link AggregatedDataPoints} instance to access aggregated data points for multiple measures,
     * each with its own missing intervals and aggregate interval.
     * @param from global start timestamp
     * @param to global end timestamp
     * @param measureRequests list of measure-specific aggregation requests
     * @param aggregateFunctions the set of aggregate functions to apply (same for all measures)
     * @return aggregated data points
     */
    AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<MeasureAggregationRequest> measureRequests, Set<String> aggregateFunctions);

    public AbstractDataset getDataset();

    public void closeConnection();

}
