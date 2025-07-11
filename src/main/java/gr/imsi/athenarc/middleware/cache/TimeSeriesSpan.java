package gr.imsi.athenarc.middleware.cache;
import java.util.Iterator;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DataPoints;

/**
 * Represents an interval in time for a single measure. To be stored in an interval tree.
 */
public interface TimeSeriesSpan extends DataPoints {
    /*
        Iterator for the objects in this time series span.
     */
    Iterator iterator(long from, long to);

    /**
     * The number of time series points fetched form the database behind every data point included in this time series span.
     */
    int getCount();

    /*
        Calculate the memory size of this span.
     */
    long calculateDeepMemorySize();

    /*
        Measure corresponding to this time series span.
     */
    int getMeasure();

    /*
        Return the aggregate Interval of this span. For raw it is null;
     */
    AggregateInterval getAggregateInterval();

    /*
     *  Wethe this span is an initialization span or not.
     */
    boolean isInit();
}
