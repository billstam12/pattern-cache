package gr.imsi.athenarc.middleware.cache;
import java.util.Iterator;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

/**
 * Represents an interval in time for a single measure. To be stored in an interval tree.
 *
 * <h2>Iterator contract — flyweight semantics</h2>
 * All span implementations of {@link #iterator(long, long)} return a flyweight: the
 * {@code Iterator} instance and the data point it yields ({@code AggregatedDataPoint}
 * for aggregate spans, {@code DataPoint} for raw spans) are the same object, so each
 * call to {@code next()} mutates the previously returned reference in place.
 *
 * Consequences for callers:
 * <ul>
 *   <li>Do <b>not</b> retain a reference to a yielded data point past the next {@code next()} call.</li>
 *   <li>Do <b>not</b> stash yielded data points in a collection without copying them, e.g. via
 *       {@code ImmutableAggregatedDataPoint.fromAggregatedDataPoint(...)} or a fresh
 *       {@code ImmutableDataPoint(...)} for raw points.</li>
 *   <li>For aggregate spans, {@code getStats()} on a yielded point allocates a fresh view that
 *       captures the current index, so it is safe to retain a {@code Stats} reference across
 *       iteration steps.</li>
 * </ul>
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

    void addAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint);

    /* 
     * Rolls up the timeseries span into one with coarser granularity
     */
    TimeSeriesSpan rollUp(AggregateInterval targetInterval);

    int getSize();

    /**
     * Orders spans by {@code from}, then {@code to}, then {@code aggregateInterval}
     * (coarse first; {@code null} ahead of non-null), then {@code measure}. The
     * aggregate/measure tie-break makes spans with identical ranges but different
     * resolutions distinct keys, so they coexist in an interval tree instead of
     * collapsing into one node.
     */
    @Override
    default int compareTo(TimeInterval o) {
        if (getFrom() > o.getFrom()) return 1;
        if (getFrom() < o.getFrom()) return -1;
        if (getTo() > o.getTo()) return 1;
        if (getTo() < o.getTo()) return -1;
        if (!(o instanceof TimeSeriesSpan)) return 0;
        TimeSeriesSpan other = (TimeSeriesSpan) o;
        AggregateInterval a = getAggregateInterval();
        AggregateInterval b = other.getAggregateInterval();
        if (a != b) {
            if (a == null) return -1;
            if (b == null) return 1;
            int aggCmp = Long.compare(
                    a.toDuration().toMillis(),
                    b.toDuration().toMillis());
            if (aggCmp != 0) return aggCmp;
        }
        return Integer.compare(getMeasure(), other.getMeasure());
    }
}
