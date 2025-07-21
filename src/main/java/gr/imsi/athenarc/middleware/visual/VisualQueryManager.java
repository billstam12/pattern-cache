package gr.imsi.athenarc.middleware.visual;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisualQueryManager {
    private static final Logger LOG = LoggerFactory.getLogger(VisualQueryManager.class);

    private final TimeSeriesCache cache;
    private final VisualQueryExecutor queryExecutor;
    private final DataProcessor dataProcessor;
    private final PrefetchManager prefetchManager;
    private final DataSource dataSource;
    private final String method;

    public VisualQueryManager(DataSource dataSource, TimeSeriesCache cache,
                             int dataReductionFactor, int initialAggregationFactor, double prefetchingFactor, 
                             String method, boolean calendarAlignment) {
        this.cache = cache;
        this.dataSource = dataSource;
        // Initialize components for visual queries
        this.dataProcessor = new DataProcessor(dataSource, dataReductionFactor, method, calendarAlignment);
        this.queryExecutor = new VisualQueryExecutor(dataSource, initialAggregationFactor);
        this.prefetchManager = new PrefetchManager(dataSource, prefetchingFactor, cache, dataProcessor);
        this.method = method;
    }

    public VisualQueryResults executeQuery(VisualQuery query) {
        return queryExecutor.executeQuery(query, cache, dataProcessor, prefetchManager);
    }
    
    
    /**
     * Get the underlying data source.
     * @return The data source
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    public String getMethod() {
        return method;
    }
}
