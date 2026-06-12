package gr.imsi.athenarc.middleware.pattern;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;

/**
 * Common entry point for pattern query executors. Held by {@link
 * PatternQueryManager} as a method reference so the concrete executor (full
 * or scoped) stays internal to the manager — callers don't depend on it.
 */
@FunctionalInterface
interface PatternExecutor {
    PatternQueryResults executeQuery(PatternQuery query, DataSource dataSource, TimeSeriesCache cache,
                                     String method, boolean adaptation,
                                     int initialAggregationFactor,
                                     boolean calendarAlignment,
                                     int dataReductionFactor,
                                     boolean relaxedCacheReuse,
                                     int maxRefinementSteps);
}
