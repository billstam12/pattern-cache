package gr.imsi.athenarc.middleware.manager.visual;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;

public class VisualQueryManager {

    private final TimeSeriesCache cache;
    private final QueryExecutor queryExecutor;
    private final DataProcessor dataProcessor;
    private final PrefetchManager prefetchManager;

    public VisualQueryManager(DataSource dataSource, TimeSeriesCache cache,
                             int dataReductionFactor, int initialAggregationFactor, int prefetchingFactor) {
        this.cache = cache;
        // Initialize components for visual queries
        this.dataProcessor = new DataProcessor(dataSource, dataReductionFactor);
        this.queryExecutor = new QueryExecutor(dataSource, initialAggregationFactor);
        this.prefetchManager = new PrefetchManager(dataSource, prefetchingFactor, cache, dataProcessor);
    }

    public VisualQueryResults executeQuery(VisualQuery query) {
        return queryExecutor.executeQuery(query, cache, dataProcessor, prefetchManager);
    }
}
