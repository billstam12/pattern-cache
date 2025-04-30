package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
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
        int measureCount = dataset.getHeader().length;
        
        // Calculate per-measure memory budget
        long targetMemoryBytes = (long)(maxMemoryBytes * memoryUtilizationTarget);
        long perMeasureBytes = targetMemoryBytes / measureCount;
        
        LOG.info("Dataset spans {} ms with {} measures", datasetTimeRange, measureCount);
        LOG.info("Memory budget per measure: {} bytes", perMeasureBytes);
        
        List<Integer> datasetMeasures = dataset.getMeasures();
         // Determine appropriate aggregation intervals
        Map<Integer, AggregateInterval> aggregateIntervals = calculateOptimalAggregationIntervals(
            datasetMeasures, perMeasureBytes, datasetTimeRange);
    
        // Create a single query with the calculated intervals for all measures
        VisualQuery query = VisualQuery.builder()
            .withMeasures(datasetMeasures)
            .withTimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo())
            .withViewPort(1000, 500)  // Default viewport
            .withAccuracy(0.95)        // Use default accuracy
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
        List<Integer> measures, long bytesPerMeasure, long datasetTimeRange) {
        
        Map<Integer, AggregateInterval> intervals = new HashMap<>();
        
        // Estimate bytes per data point (this is an approximation)
        long bytesPerPoint = calculateDeepMemorySize(); // Adjust based on actual memory profile
        
        for (Integer measureId : measures) {
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
        AggregateInterval[] standardIntervals = {
            AggregateInterval.of(1, ChronoUnit.MINUTES),
            AggregateInterval.of(5, ChronoUnit.MINUTES),
            AggregateInterval.of(15, ChronoUnit.MINUTES),
            AggregateInterval.of(1, ChronoUnit.HOURS),
            AggregateInterval.of(6, ChronoUnit.HOURS),
            AggregateInterval.of(1, ChronoUnit.DAYS),
            AggregateInterval.of(1, ChronoUnit.WEEKS)
        };
        
        // Find smallest standard interval that's larger than our minimum
        for (AggregateInterval interval : standardIntervals) {
            if (interval.toDuration().toMillis() >= minIntervalMs) {
                return interval;
            }
        }

        // By default return the largest interval (1 week)
        return AggregateInterval.of(1, ChronoUnit.WEEKS);
    }
    
    @Override
    public String getDescription() {
        return String.format("Memory-bounded initialization (limit: %d bytes, target: %.0f%%)", 
                maxMemoryBytes, memoryUtilizationTarget * 100);
    }


    private long calculateDeepMemorySize() {
        // Memory overhead for an object in a 64-bit JVM
        final int OBJECT_OVERHEAD = 16;
        // Memory overhead for an array in a 64-bit JVM
        final int ARRAY_OVERHEAD = 20;
        // Memory usage of int in a 64-bit JVM
        final int INT_SIZE = 4;
        // Memory usage of long in a 64-bit JVM
        final int LONG_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;


        long aggregatesMemory = REF_SIZE + ARRAY_OVERHEAD + AggregateTimeSeriesSpan.getAggSize() * (REF_SIZE + ARRAY_OVERHEAD + ((long)  AggregateTimeSeriesSpan.getAggSize() * LONG_SIZE));

        long countsMemory = REF_SIZE + ARRAY_OVERHEAD + ((long) INT_SIZE);

        long aggregateIntervalMemory = 2 * REF_SIZE + OBJECT_OVERHEAD + LONG_SIZE;

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD +
                aggregatesMemory + countsMemory + LONG_SIZE + INT_SIZE + aggregateIntervalMemory;


        return deepMemorySize;
    }

}
