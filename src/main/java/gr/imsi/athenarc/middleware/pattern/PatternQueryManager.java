package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementScope;

public class PatternQueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;
    private final String method;
    private final boolean adaptation;
    private final int initialAggregationFactor;
    private final boolean calendarAlignment;
    private final int dataReductionFactor;
    private final boolean relaxedCacheReuse;
    private final int maxRefinementSteps;
    private final PatternExecutor executor;

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation) {
        this(dataSource, cache, method, adaptation, 4, true, 4, false, RefinementScope.SCOPED,  20);
    }

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation,
                               int initialAggregationFactor) {
        this(dataSource, cache, method, adaptation, initialAggregationFactor, true, 4, false, RefinementScope.SCOPED, 20);
    }

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation,
                               int initialAggregationFactor, boolean calendarAlignment) {
        this(dataSource, cache, method, adaptation, initialAggregationFactor, calendarAlignment, 4, false, RefinementScope.SCOPED, 20);
    }

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation,
                               int initialAggregationFactor, boolean calendarAlignment, int dataReductionFactor) {
        this(dataSource, cache, method, adaptation, initialAggregationFactor, calendarAlignment, dataReductionFactor, false, RefinementScope.SCOPED, 20);
    }

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation,
                               int initialAggregationFactor, boolean calendarAlignment, int dataReductionFactor,
                               boolean relaxedCacheReuse) {
        this(dataSource, cache, method, adaptation, initialAggregationFactor, calendarAlignment, dataReductionFactor,
                relaxedCacheReuse, RefinementScope.SCOPED, 20);
    }

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation,
                               int initialAggregationFactor, boolean calendarAlignment, int dataReductionFactor,
                               boolean relaxedCacheReuse, RefinementScope scope, int maxRefinementSteps) {
        this.dataSource = dataSource;
        this.cache = cache;
        this.method = method;
        this.adaptation = adaptation;
        this.initialAggregationFactor = Math.max(1, initialAggregationFactor);
        this.calendarAlignment = calendarAlignment;
        this.dataReductionFactor = Math.max(1, dataReductionFactor);
        this.relaxedCacheReuse = relaxedCacheReuse;
        this.maxRefinementSteps = Math.max(0, maxRefinementSteps);
        this.executor = selectExecutor(scope);
    }

    private static PatternExecutor selectExecutor(RefinementScope scope) {
        boolean full = scope == RefinementScope.FULL;
         return full
                        ? FullPatternQueryExecutor::executePatternQueryWithCache
                        : ScopedPatternQueryExecutor::executePatternQueryWithCache;
    }

    public PatternQueryResults executeQuery(PatternQuery query) {
        return executor.executeQuery(
                query, dataSource, cache, method, adaptation,
                initialAggregationFactor, calendarAlignment, dataReductionFactor, relaxedCacheReuse,
                maxRefinementSteps);
    }
}
