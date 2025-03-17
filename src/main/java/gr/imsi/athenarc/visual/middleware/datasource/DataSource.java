package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;

/**
 * Represents a time series data source
 */
public interface DataSource {

    /**
     * Returns an {@link AggregatedDataPoints} instance to access aggregated data points in the time series,
     * that have a timestamp greater than or equal to the startTimestamp, and less than or equal to the endTimestamp,
     * aggregated in the specified time unit.
     * @param from
     * @param to
     * @param measure
     * @param chronoUnit
     * @return
     */
    AggregatedDataPoints getSlopeDataPoints(long from, long to, int measure, AggregateInterval aggregateInterval);

    public AbstractDataset getDataset();

    public void closeConnection();

}
