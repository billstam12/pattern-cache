package gr.imsi.athenarc.middleware.examples;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import gr.imsi.athenarc.middleware.cache.initialization.FullDatasetInitializationPolicy;
import gr.imsi.athenarc.middleware.cache.initialization.MemoryBoundedInitializationPolicy;
import gr.imsi.athenarc.middleware.cache.initialization.RecentDataInitializationPolicy;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.manager.CacheManager;

public class CacheInitializationExample {
    
    public static void main(String[] args) {
        // Create data source (example, replace with your actual data source)
        DataSource dataSource = createDataSource();
        
        // Example 1: Using full dataset initialization with 5-minute intervals for all measures
        Map<Integer, AggregateInterval> aggregateIntervals = new HashMap<>();
        for (Integer measureId : dataSource.getDataset().getMeasures()) {
            aggregateIntervals.put(measureId, AggregateInterval.of(5, ChronoUnit.MINUTES));
        }
        
        CacheManager manager1 = CacheManager.builder(dataSource)
            .withInitializationPolicy(new FullDatasetInitializationPolicy(aggregateIntervals))
            .build();
        
        // Example 2: Using recent data initialization (last 24 hours)
        CacheManager manager2 = CacheManager.builder(dataSource)
            .withInitializationPolicy(new RecentDataInitializationPolicy(
                Duration.ofHours(24), aggregateIntervals))
            .build();
        
        // Example 3: Using memory-bounded initialization
        CacheManager manager3 = CacheManager.builder(dataSource)
            .withInitializationPolicy(new MemoryBoundedInitializationPolicy(
                1024 * 1024 * 1024, // 1GB memory limit
                0.8))              // 80% target utilization
            .build();
        
        // Example 4: Create manager first, then initialize cache later
        CacheManager manager4 = CacheManager.builder(dataSource).build();
        
        // Later, initialize the cache with a policy
        manager4.initializeCache(new MemoryBoundedInitializationPolicy(
            512 * 1024 * 1024, // 512MB memory limit
            0.9));             // 90% target utilization
    }
    
    private static DataSource createDataSource() {
        // Replace with your actual data source creation
        return null;
    }
}
