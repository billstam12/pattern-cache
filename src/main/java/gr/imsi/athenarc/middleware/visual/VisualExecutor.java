package gr.imsi.athenarc.middleware.visual;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;

/**
 * Common entry point for visual query executors. Held by {@link VisualQueryManager}
 * as a method reference so the concrete executor type (whole-scope or scoped/
 * ambiguous) stays internal to the manager — callers don't depend on it.
 */
@FunctionalInterface
interface VisualExecutor {
    VisualQueryResults executeQuery(VisualQuery query, TimeSeriesCache cache,
                                    DataProcessor dataProcessor, PrefetchManager prefetchManager);
}
