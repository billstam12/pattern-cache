package gr.imsi.athenarc.middleware.visual;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.refinement.RefinementScope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisualQueryManager {
    private static final Logger LOG = LoggerFactory.getLogger(VisualQueryManager.class);

    private final TimeSeriesCache cache;
    private final VisualExecutor queryExecutor;
    private final DataProcessor dataProcessor;
    private final PrefetchManager prefetchManager;
    private final DataSource dataSource;
    private final String method;

    public VisualQueryManager(DataSource dataSource, TimeSeriesCache cache,
                             int dataReductionFactor, int initialAggregationFactor, double prefetchingFactor,
                             String method, boolean calendarAlignment) {
        this(dataSource, cache, dataReductionFactor, initialAggregationFactor, prefetchingFactor,
                method, calendarAlignment, RefinementScope.SCOPED, 20);
    }

    public VisualQueryManager(DataSource dataSource, TimeSeriesCache cache,
                             int dataReductionFactor, int initialAggregationFactor, double prefetchingFactor,
                             String method, boolean calendarAlignment, RefinementScope scope,
                             int maxRefinementSteps) {
        this.cache = cache;
        this.dataSource = dataSource;
        this.dataProcessor = new DataProcessor(dataSource, dataReductionFactor, method, calendarAlignment);
        this.prefetchManager = new PrefetchManager(dataSource, prefetchingFactor, cache, dataProcessor);
        this.method = method;
        // Scope dispatch lives here — once. Each executor is a standalone class
        // owning its own fetch ladder; the manager just picks one and forgets.
        switch (scope) {
            case SCOPED: {
                ScopedVisualQueryExecutor exec =
                        new ScopedVisualQueryExecutor(dataSource, initialAggregationFactor, dataReductionFactor,
                                maxRefinementSteps);
                this.queryExecutor = exec::executeQuery;
                break;
            }
            case FULL:
            default: {
                FullVisualQueryExecutor exec =
                        new FullVisualQueryExecutor(dataSource, initialAggregationFactor, dataReductionFactor);
                this.queryExecutor = exec::executeQuery;
                break;
            }
        }
    }

    public VisualQueryResults executeQuery(VisualQuery query) {
        return queryExecutor.executeQuery(query, cache, dataProcessor, prefetchManager);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public String getMethod() {
        return method;
    }
}
