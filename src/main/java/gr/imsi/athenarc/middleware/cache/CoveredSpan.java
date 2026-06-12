package gr.imsi.athenarc.middleware.cache;

/**
 * A cache span paired with the time sub-range it owns inside a query.
 *
 * <p>Returned by {@link TimeSeriesCache#getCoarsestPerRegionCoverage} when the
 * coarsest-per-region selection policy is on. The cache picks one cached span
 * per contiguous sub-range of the query, choosing the coarsest admissible
 * resolution that covers it. Consumers (pattern populate, visual populate)
 * iterate the {@code span} but emit data points only for sub-buckets whose
 * {@code [from, to]} falls inside {@code [ownedFrom, ownedTo]}; the rest of
 * the span belongs to a different {@code CoveredSpan} entry (or to a missing
 * region that gets fetched).
 */
public final class CoveredSpan {
    private final TimeSeriesSpan span;
    private final long ownedFrom;
    private final long ownedTo;

    public CoveredSpan(TimeSeriesSpan span, long ownedFrom, long ownedTo) {
        this.span = span;
        this.ownedFrom = ownedFrom;
        this.ownedTo = ownedTo;
    }

    public TimeSeriesSpan getSpan() { return span; }
    public long getOwnedFrom() { return ownedFrom; }
    public long getOwnedTo() { return ownedTo; }

    @Override
    public String toString() {
        return String.format("CoveredSpan[%d-%d, agg=%s, ownedRange=%d-%d]",
                span.getFrom(), span.getTo(), span.getAggregateInterval(), ownedFrom, ownedTo);
    }
}
