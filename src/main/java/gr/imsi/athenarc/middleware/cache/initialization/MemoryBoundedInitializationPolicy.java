package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.util.HashMap;
import java.util.Map;

/**
 * A cache initialization policy that attempts to load as much data as possible
 * while remaining within memory constraints.
 * 
 * This policy automatically determines appropriate aggregation levels based on the
 * dataset characteristics and available memory.
 */
public class MemoryBoundedInitializationPolicy implements CacheInitializationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryBoundedInitializationPolicy.class);
    
    private final long maxMemoryBytes;
    private final double memoryUtilizationTarget; // between 0.0 and 1.0
    
    /**
     * Creates a memory-bounded initialization policy.
     * 
     * @param maxMemoryBytes Maximum memory in bytes to utilize for cache
     * @param memoryUtilizationTarget Target memory utilization (0.0-1.0)
     */
    public MemoryBoundedInitializationPolicy(long maxMemoryBytes, double memoryUtilizationTarget) {
        this(maxMemoryBytes, memoryUtilizationTarget, false, 1);
    }
    
    /**
     * Creates a memory-bounded initialization policy with parallel execution options.
     * 
     * @param maxMemoryBytes Maximum memory in bytes to utilize for cache
     * @param memoryUtilizationTarget Target memory utilization (0.0-1.0)
     * @param useParallelExecution Whether to use parallel execution
     * @param maxThreads Maximum number of threads for parallel execution
     */
    public MemoryBoundedInitializationPolicy(
            long maxMemoryBytes, 
            double memoryUtilizationTarget,
            boolean useParallelExecution,
            int maxThreads) {
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryUtilizationTarget = memoryUtilizationTarget;

        if (memoryUtilizationTarget <= 0.0 || memoryUtilizationTarget > 1.0) {
            throw new IllegalArgumentException("Memory utilization target must be between 0.0 and 1.0");
        }
    }
    
    @Override
    public void initialize(TimeSeriesCache cache, VisualQueryManager visualQueryManager) {
        LOG.info("Initializing cache with memory-bounded approach (limit: {} bytes, utilization: {}%)", 
                maxMemoryBytes, memoryUtilizationTarget * 100);
                
        AbstractDataset dataset = visualQueryManager.getDataSource().getDataset();
        long datasetTimeRange = dataset.getTimeRange().getTo() - dataset.getTimeRange().getFrom();
        int measureCount = dataset.getMeasures().size();
        
        // Calculate per-measure memory budget
        long targetMemoryBytes = (long)(maxMemoryBytes * memoryUtilizationTarget);
        long perMeasureBytes = targetMemoryBytes / measureCount;
        
        LOG.info("Dataset spans {} ms with {} measures", datasetTimeRange, measureCount);
        LOG.info("Memory budget per measure: {} bytes", perMeasureBytes);
        
        // Determine appropriate aggregation intervals
        Map<Integer, AggregateInterval> aggregateIntervals = calculateOptimalAggregationIntervals(
                dataset, perMeasureBytes, datasetTimeRange);
        
        // Create a single query with the calculated intervals for all measures
        VisualQuery query = VisualQuery.builder()
            .withMeasures(dataset.getMeasures())
            .withTimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo())
            .withViewPort(1000, 500)  // Default viewport
            .withAccuracy(1.0)        // Use highest accuracy
            .withMeasureAggregateIntervals(aggregateIntervals)
            .build();
        
        try {
            visualQueryManager.executeQuery(query);
            LOG.info("Successfully pre-loaded all measures with memory-optimized intervals");
        } catch (Exception e) {
            LOG.error("Failed to pre-load measures: {}", e.getMessage(), e);
        }
        
        LOG.info("Memory-bounded initialization completed");
    }

    
    /**
     * Calculate optimal aggregation intervals for each measure based on memory constraints.
     */
    private Map<Integer, AggregateInterval> calculateOptimalAggregationIntervals(
        AbstractDataset dataset, long bytesPerMeasure, long datasetTimeRange) {
        
        Map<Integer, AggregateInterval> intervals = new HashMap<>();
        
        // Estimate bytes per data point (this is an approximation)
        long bytesPerPoint = 40; // Adjust based on actual memory profile
        
        for (Integer measureId : dataset.getMeasures()) {
            // Calculate how many points we can store with our memory budget
            long maxDataPoints = bytesPerMeasure / bytesPerPoint;
            
            // Calculate minimum aggregation interval needed to fit within memory
            long minIntervalMs = datasetTimeRange / maxDataPoints;
            
            // Round up to a sensible interval (1min, 5min, 15min, 1hr, etc.)
            AggregateInterval interval = selectAppropriateInterval(minIntervalMs);
            
            intervals.put(measureId, interval);
            LOG.info("Measure {}: Can store ~{} points, using interval {}", 
                    measureId, maxDataPoints, interval);
        }
        
        return intervals;
    }
    
    /**
     * Select an appropriate standard interval based on a minimum millisecond value.
     */
    private AggregateInterval selectAppropriateInterval(long minIntervalMs) {
        // Standard intervals in milliseconds
        long[] standardIntervals = {
            60_000,         // 1 minute
            300_000,        // 5 minutes
            900_000,        // 15 minutes
            3_600_000,      // 1 hour
            21_600_000,     // 6 hours
            86_400_000,     // 1 day
            604_800_000     // 1 week
        };
        
        // Find smallest standard interval that's larger than our minimum
        for (long intervalMs : standardIntervals) {
            if (intervalMs >= minIntervalMs) {
                return AggregateInterval.fromMillis(intervalMs);
            }
        }
        
        // If we need an even larger interval, use weeks
        long weeks = (minIntervalMs + 604_800_000 - 1) / 604_800_000; // ceiling division
        return AggregateInterval.fromMillis(weeks * 604_800_000);
    }
    
    @Override
    public String getDescription() {
        return String.format("Memory-bounded initialization (limit: %d bytes, target: %.0f%%)", 
                maxMemoryBytes, memoryUtilizationTarget * 100);
    }
}
