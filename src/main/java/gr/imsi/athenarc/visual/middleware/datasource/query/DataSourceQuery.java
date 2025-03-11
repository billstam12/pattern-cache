package gr.imsi.athenarc.visual.middleware.datasource.query;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

/**
 * Represents a time series data source query
 */
public abstract class DataSourceQuery implements TimeInterval {

    final long from;
    final long to;
   
    public DataSourceQuery(long from, long to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }
}


