package gr.imsi.athenarc.middleware.cache.initialization;

import gr.imsi.athenarc.middleware.cache.CacheManager;

/**
 * Interface for different cache initialization strategies.
 * Implementations define how the cache should be pre-populated with data.
 */
public interface CacheInitializationPolicy {
    
    /**
     * Initialize the cache according to the specific policy.
     * 
     * @param cacheManager The cache to initialize
     */
    void initialize(CacheManager cacheManager);
    
    /**
     * Gets a human-readable description of the initialization policy.
     * 
     * @return Description of the policy
     */
    String getDescription();
}
