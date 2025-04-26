package gr.imsi.athenarc.middleware.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.initialization.CacheInitializationPolicy;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.manager.pattern.PatternQueryManager;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;


/**
 * Cache manager that handles different types of queries and uses a unified cache.
 */
public class CacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);
    
    private final TimeSeriesCache cache;
    private final PatternQueryManager patternQueryManager;
    private final VisualQueryManager visualQueryManager;
    
    // Private constructor used by builder
    private CacheManager(TimeSeriesCache cache, 
                        PatternQueryManager patternQueryManager, 
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
     * Manually run cache initialization using the provided policy.
     * This allows explicit cache warming after the CacheManager has been created.
     * 
     * @param policy The initialization policy to apply
     */
    public void initializeCache(CacheInitializationPolicy policy) {
        LOG.info("Initializing cache using policy: {}", policy.getDescription());
        policy.initialize(cache, visualQueryManager);
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
    public static CacheManager createDefault(DataSource dataSource) {
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
        private CacheInitializationPolicy initializationPolicy = null;

        public Builder(DataSource dataSource) {
            this.dataSource = dataSource;
            // Initialize cache structure for all measures
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
        
        /**
         * Sets a cache initialization policy that will be applied during build.
         * 
         * @param policy The initialization policy to use
         * @return The builder instance
         */
        public Builder withInitializationPolicy(CacheInitializationPolicy policy) {
            this.initializationPolicy = policy;
            return this;
        }
        
        public CacheManager build() {
            // Create a CacheManager that uses our unified cache
            
            PatternQueryManager patternQueryManager = 
                new PatternQueryManager(dataSource, cache);
                
            VisualQueryManager visualQueryManager = 
                new VisualQueryManager(dataSource, cache, dataReductionFactor, initialAggregationFactor, prefetchingFactor);
                
            CacheManager manager = new CacheManager(cache, patternQueryManager, visualQueryManager);
            
            // Apply initialization policy if one was specified
            if (initializationPolicy != null) {
                LOG.info("Applying cache initialization policy: {}", initializationPolicy.getDescription());
                initializationPolicy.initialize(cache, visualQueryManager);
            }
            
            return manager;
        }
    }
}
