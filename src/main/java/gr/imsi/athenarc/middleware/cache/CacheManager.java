package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.initialization.CacheInitializationPolicy;
import gr.imsi.athenarc.middleware.pattern.PatternQueryManager;
import gr.imsi.athenarc.middleware.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.datasource.DataSource;
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
    private final DataSource dataSource;
    private final CacheMemoryManager memoryManager;

    // Private constructor used by builder
    private CacheManager(TimeSeriesCache cache, 
                        DataSource dataSource,
                        PatternQueryManager patternQueryManager, 
                        VisualQueryManager visualQueryManager,
                        CacheMemoryManager memoryManager) {
        this.cache = cache;
        this.dataSource = dataSource;
        this.patternQueryManager = patternQueryManager;
        this.visualQueryManager = visualQueryManager;
        this.memoryManager = memoryManager;
        
        // Connect cache and memory manager if one is provided
        if (memoryManager != null) {
            cache.setMemoryManager(memoryManager);
        }
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
        // Check memory usage before executing query
        if (memoryManager != null) {
            memoryManager.checkMemoryAndEvict();
        }
        
        return patternQueryManager.executeQuery(query);
    }
    
    /**
     * Execute a visualization query for multiple measures.
     *
     * @param query the visualization query
     * @return map of measure IDs to their aggregated data points
     */
    private VisualQueryResults executeVisualQuery(VisualQuery query) {
        // Check memory usage before executing query
        if (memoryManager != null) {
            memoryManager.checkMemoryAndEvict();
        }
        
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
        policy.initialize(this);
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
        return builder(dataSource).withMaxMemory(100*1024*1024).build();
    }
    
    /**
     * Get access to the unified cache
     * @return The TimeSeriesCache used by this query manager
     */
    public TimeSeriesCache getCache() {
        return cache;
    }
    
    /**
     * Get the cache memory manager if available
     * @return The CacheMemoryManager or null if not configured
     */
    public CacheMemoryManager getMemoryManager() {
        return memoryManager;
    }
    
    /**
     * Builder class for QueryManager that allows configuring specific components
     */
    public static class Builder {
        private final DataSource dataSource;
        private TimeSeriesCache cache = new TimeSeriesCache();
        private int dataReductionFactor = 4;
        private int initialAggregationFactor = 4;
        private int prefetchingFactor = 0;
        private CacheInitializationPolicy initializationPolicy = null;
        private Long maxMemoryBytes = null;
        private Double memoryUtilizationThreshold = null;
        private String method = "m4_inf";
        private boolean calendarAlignment = true;
        private boolean adaptation = true;

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

        public Builder withMethod(String method) {
            this.method = method;
            return this;
        }

        public Builder withCalendarAlignment(boolean calendarAlignment) {
            this.calendarAlignment = calendarAlignment;
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
        
        /**
         * Sets the maximum memory (in bytes) the cache should use.
         * This will enable memory management for the cache.
         * 
         * @param maxMemoryBytes Maximum memory in bytes
         * @return The builder instance
         */
        public Builder withMaxMemory(long maxMemoryBytes) {
            this.maxMemoryBytes = maxMemoryBytes;
            return this;
        }

        /**
         * Enables or disables adaptation (dynamic aggregation factor adjustment)
         * during pattern query execution.
         * 
         * @param adaptation True to enable adaptation, false to disable
         * @return The builder instance
         */
        public Builder withAdaptation(boolean adaptation) {
            this.adaptation = adaptation;
            return this;
        }
        
        /**
         * Sets the memory utilization threshold (0.0-1.0) at which to start
         * evicting cached items.
         * 
         * @param threshold Threshold between 0.0 and 1.0
         * @return The builder instance
         */
        public Builder withMemoryUtilizationThreshold(double threshold) {
            if (threshold <= 0.0 || threshold > 1.0) {
                throw new IllegalArgumentException(
                    "Memory utilization threshold must be between 0.0 and 1.0");
            }
            this.memoryUtilizationThreshold = threshold;
            return this;
        }
        
        public CacheManager build() {
            // Create memory manager if max memory is specified
            CacheMemoryManager memoryManager = null;
            if (maxMemoryBytes != null) {
                double threshold = memoryUtilizationThreshold != null ? 
                    memoryUtilizationThreshold : 0.8;
                memoryManager = new CacheMemoryManager(
                    cache, maxMemoryBytes, threshold, 10, true);
                LOG.info("Created cache memory manager with {} MB max memory and {}% threshold",
                    maxMemoryBytes/(1024*1024), threshold*100);
            }
            
            // Create a CacheManager that uses our unified cache
            PatternQueryManager patternQueryManager = 
                new PatternQueryManager(dataSource, cache, method, adaptation);
                
            VisualQueryManager visualQueryManager = 
                new VisualQueryManager(dataSource, cache, dataReductionFactor, initialAggregationFactor, prefetchingFactor, method, calendarAlignment);
                
            CacheManager manager = new CacheManager(cache, dataSource, patternQueryManager, visualQueryManager, memoryManager);

            // Apply initialization policy if one was specified
            if (initializationPolicy != null) {
                LOG.info("Applying cache initialization policy: {}", initializationPolicy.getDescription());
                initializationPolicy.initialize(manager);
                
                // After initialization, update memory usage tracking
                if (memoryManager != null) {
                    memoryManager.updateCurrentMemoryUsage();
                    LOG.info("Initial cache memory usage: {} MB / {} MB ({}%)", 
                            memoryManager.getCurrentMemoryBytes()/(1024*1024),
                            memoryManager.getMaxMemoryBytes()/(1024*1024),
                            memoryManager.getMemoryUtilizationPercentage());
                }
            }
            
            return manager;
        }
    }

    public PatternQueryManager getPatternQueryManager() {
        return patternQueryManager;
    }

    public VisualQueryManager getVisualQueryManager() {
        return visualQueryManager;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
