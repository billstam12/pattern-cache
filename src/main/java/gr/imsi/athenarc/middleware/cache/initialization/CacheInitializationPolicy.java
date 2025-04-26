package gr.imsi.athenarc.middleware.cache.initialization;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;

/**
 * Interface for different cache initialization strategies.
 * Implementations define how the cache should be pre-populated with data.
 */
public interface CacheInitializationPolicy {
    
    /**
     * Initialize the cache according to the specific policy.
     * 
     * @param cache The cache to initialize
     * @param visualQueryManager The manager used to execute visual queries
     */
    void initialize(TimeSeriesCache cache, VisualQueryManager visualQueryManager);
    
    /**
     * Gets a human-readable description of the initialization policy.
     * 
     * @return Description of the policy
     */
    String getDescription();
}
