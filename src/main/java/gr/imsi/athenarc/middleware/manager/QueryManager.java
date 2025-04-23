package gr.imsi.athenarc.middleware.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.manager.pattern.QueryExecutor;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;


/**
 * Query manager that handles different types of queries and uses a unified cache.
 */
public class QueryManager {
    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    
    private final TimeSeriesCache cache;
    private final QueryExecutor patternQueryManager;
    private final VisualQueryManager visualQueryManager;
    
    // Private constructor used by builder
    private QueryManager(TimeSeriesCache cache, 
                        QueryExecutor patternQueryManager, 
                        VisualQueryManager visualQueryManager) {
        this.cache = cache;
        this.patternQueryManager = patternQueryManager;
        this.visualQueryManager = visualQueryManager;
    }
    
    /**
     * Execute a query based on its type.
     *
     * @param query the query to execute
     * @return the result of the query execution
     */
    public QueryResults executeQuery(Query query) {
        LOG.info("Executing {} query from {} to {}", 
                query.getType(), query.getFrom(), query.getTo());
        
        switch (query.getType()) {
            case PATTERN:
                return executePatternQuery((PatternQuery) query);
            case VISUALIZATION:
                return executeVisualQuery((VisualQuery) query);
            default:
                throw new UnsupportedOperationException("Unsupported query type: " + query.getType());
        }
    }
    
    /**
     * Execute a pattern matching query.
     *
     * @param query the pattern query
     * @return the result of pattern matching
     */
    private PatternQueryResults executePatternQuery(PatternQuery query) {
        LOG.debug("Handling pattern query for measure {}", query.getMeasures());
        return patternQueryManager.executeQuery(query);
    }
    
    /**
     * Execute a visualization query for multiple measures.
     *
     * @param query the visualization query
     * @return map of measure IDs to their aggregated data points
     */
    private VisualQueryResults executeVisualQuery(VisualQuery query) {
        LOG.debug("Handling visualization query for {} measures", query.getMeasures().size());
    
        return visualQueryManager.executeQuery(query);
    }
    
    /**
     * Creates a new builder for QueryManager
     * @param dataSource The data source to use
     * @return A new builder instance
     */
    public static Builder builder(DataSource dataSource) {
        return new Builder(dataSource);
    }
    
    /**
     * Creates a new QueryManager with default settings
     * @param dataSource The data source to use
     * @return A new QueryManager instance
     */
    public static QueryManager createDefault(DataSource dataSource) {
        return builder(dataSource).build();
    }
    
    /**
     * Get access to the unified cache
     * @return The TimeSeriesCache used by this query manager
     */
    public TimeSeriesCache getCache() {
        return cache;
    }
    
    /**
     * Builder class for QueryManager that allows configuring specific components
     */
    public static class Builder {
        private final DataSource dataSource;
        private TimeSeriesCache cache = new TimeSeriesCache();
        private int dataReductionFactor = 6;
        private int initialAggregationFactor = 4;
        private int prefetchingFactor = 0;

        public Builder(DataSource dataSource) {
            this.dataSource = dataSource;
            // Initialize cache for all measures
            cache.initializeForMeasures(dataSource.getDataset().getMeasures());
        }
        
        public Builder withCache(TimeSeriesCache cache) {
            this.cache = cache;
            return this;
        }
        
        public Builder withDataReductionFactor(int factor) {
            this.dataReductionFactor = factor;
            return this;
        }
        
        public Builder withInitialAggregationFactor(int factor) {
            this.initialAggregationFactor = factor;
            return this;
        }
        
        public Builder withPrefetchingFactor(int factor) {
            this.prefetchingFactor = factor;
            return this;
        }
        
        public QueryManager build() {
            // Create a CacheManager that uses our unified cache
            
            QueryExecutor patternQueryManager = 
                new QueryExecutor(dataSource, cache);
                
            VisualQueryManager visualQueryManager = 
                new VisualQueryManager(dataSource, cache, dataReductionFactor, initialAggregationFactor, prefetchingFactor);
                
            return new QueryManager(cache, patternQueryManager, visualQueryManager);
        }
    }
}
