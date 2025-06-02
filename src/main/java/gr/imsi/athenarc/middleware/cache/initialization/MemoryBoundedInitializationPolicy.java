package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.M4StarAggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A cache initialization policy that attempts to load data within memory constraints.
 * 
 * This policy can be configured to load either:
 * 1. The entire dataset with automatically determined aggregation levels
 * 2. Only recent data with automatically determined aggregation levels
 * 
 * In both cases, the policy respects memory constraints by adjusting aggregation intervals.
 */
public class MemoryBoundedInitializationPolicy implements CacheInitializationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryBoundedInitializationPolicy.class);
    
    private final long maxMemoryBytes;
    private final double memoryUtilizationTarget; // between 0.0 and 1.0
    private final Double lookbackPercentage; // null means full dataset range, otherwise a value between 0.0 and 1.0
    
    /**
     * Creates a memory-bounded initialization policy for recent data only,
     * using a percentage of the dataset's time range, with parallel execution.
     * 
     * @param maxMemoryBytes Maximum memory in bytes to utilize for cache
     * @param memoryUtilizationTarget Target memory utilization (0.0-1.0)
     * @param lookbackPercentage Percentage of dataset's time range to load (0.0-1.0)
     * @param useParallelExecution Whether to use parallel execution
     * @param maxThreads Maximum number of threads for parallel execution
     */
    public MemoryBoundedInitializationPolicy(
            long maxMemoryBytes, 
            double memoryUtilizationTarget,
            double lookbackPercentage) {
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryUtilizationTarget = memoryUtilizationTarget;
        this.lookbackPercentage = lookbackPercentage;

        if (memoryUtilizationTarget <= 0.0 || memoryUtilizationTarget > 1.0) {
            throw new IllegalArgumentException("Memory utilization target must be between 0.0 and 1.0");
        }
        
        if (lookbackPercentage <= 0.0 || lookbackPercentage > 1.0) {
            throw new IllegalArgumentException("Lookback percentage must be between 0.0 and 1.0");
        }
    }
    
    /**
     * Creates a memory-bounded initialization policy with full configuration.
     * 
     * @param maxMemoryBytes Maximum memory in bytes to utilize for cache
     * @param memoryUtilizationTarget Target memory utilization (0.0-1.0)
     * @param lookbackPeriod How far back in time to load data (null for full dataset)     */
    public MemoryBoundedInitializationPolicy(
            long maxMemoryBytes, 
            double memoryUtilizationTarget) {
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryUtilizationTarget = memoryUtilizationTarget;
        this.lookbackPercentage = null;

        if (memoryUtilizationTarget <= 0.0 || memoryUtilizationTarget > 1.0) {
            throw new IllegalArgumentException("Memory utilization target must be between 0.0 and 1.0");
        }
    }
    
    @Override
    public void initialize(TimeSeriesCache cache, DataSource dataSource) {
        initialize(cache, dataSource, null);
    }
    
    /**
     * Initialize the cache with specific measures.
     * 
     * @param cache The time series cache to initialize
     * @param measures List of measures to initialize (null means use all measures)
     */
    public void initialize(TimeSeriesCache cache, DataSource dataSource, List<Integer> measures) {
        if (lookbackPercentage == null) {
            LOG.info("Initializing cache with full dataset using memory-bounded approach (limit: {} bytes, utilization: {}%)", 
                    maxMemoryBytes, memoryUtilizationTarget * 100);
        } else {
            LOG.info("Initializing cache with recent data for {}% of the dataset using memory-bounded approach (limit: {} bytes, utilization: {}%)", 
                lookbackPercentage, maxMemoryBytes, memoryUtilizationTarget * 100);
        }
                
        AbstractDataset dataset = dataSource.getDataset();
        // Determine time range based on mode (full or recent data)
        long endTimestamp = dataset.getTimeRange().getTo();
        long startTimestamp = dataset.getTimeRange().getFrom();
        long datasetDuration = endTimestamp - startTimestamp;
        
        if (lookbackPercentage != null) {
            long lookbackDuration = (long)(datasetDuration * lookbackPercentage);
            long recentStartTimestamp = endTimestamp - lookbackDuration;
            // Ensure we don't go before the dataset start
            startTimestamp = Math.max(recentStartTimestamp, dataset.getTimeRange().getFrom());
            LOG.info("Loading data from {} to {} (recent data mode with {}% of dataset)", 
                    startTimestamp, endTimestamp, lookbackPercentage * 100);
        } else {
            LOG.info("Loading data from {} to {} (full dataset mode)", startTimestamp, endTimestamp);
        }
        
        long datasetTimeRange = endTimestamp - startTimestamp;
        
        // Use specific measures if provided, otherwise use all dataset measures
        List<Integer> measureList = measures != null ? measures : dataset.getMeasures();
        int measureCount = measureList.size();
        
        // Calculate per-measure memory budget
        long targetMemoryBytes = (long)(maxMemoryBytes * memoryUtilizationTarget);
        long perMeasureBytes = targetMemoryBytes / measureCount;
        
        LOG.info("Selected time range spans {} ms with {} measures", datasetTimeRange, measureCount);
        LOG.info("Memory budget per measure: {} bytes", perMeasureBytes);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
        // Determine appropriate aggregation intervals
        Map<Integer, AggregateInterval> aggregateIntervals = calculateOptimalAggregationIntervals(
            measureList, perMeasureBytes, datasetTimeRange);
    
                
        for (int measure : measureList){
            List<TimeInterval> missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(startTimestamp, endTimestamp));
            missingIntervalsPerMeasure.put(measure, missingIntervals);
        }
        
        Set<String> aggregateFunctions = new HashSet<>(Arrays.asList("min", "max", "first", "last"));

        AggregatedDataPoints missingDataPoints = 
            dataSource.getAggregatedDataPoints(startTimestamp, endTimestamp, missingIntervalsPerMeasure, aggregateIntervals, aggregateFunctions);

        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createM4StarAggregate(missingDataPoints, missingIntervalsPerMeasure, aggregateIntervals);
        
        for(List<TimeSeriesSpan> spans : timeSeriesSpans.values()) {
            cache.addToCache(spans);
        }

        try {
            LOG.info("Successfully pre-loaded {} measures with memory-optimized intervals", measureList.size());
        } catch (Exception e) {
            LOG.error("Failed to pre-load measures: {}", e.getMessage(), e);
        }
        
        LOG.info("Memory-bounded initialization completed");
    }

    
    @Override
    public String getDescription() {
        if (lookbackPercentage != null) {
            return String.format("Memory-bounded recent data initialization for the last %.1f%% of dataset (limit: %d bytes, target: %.0f%%)", 
                    lookbackPercentage * 100, maxMemoryBytes, memoryUtilizationTarget * 100);
        } else {
            return String.format("Memory-bounded recent data initialization for the whole dataset (limit: %d bytes, target: %.0f%%)", 
                maxMemoryBytes, memoryUtilizationTarget * 100);
        }
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


        long aggregatesMemory = REF_SIZE + ARRAY_OVERHEAD + M4StarAggregateTimeSeriesSpan.getAggSize() * (REF_SIZE + ARRAY_OVERHEAD + ((long)  M4StarAggregateTimeSeriesSpan.getAggSize() * LONG_SIZE));

        long countsMemory = REF_SIZE + ARRAY_OVERHEAD + ((long) INT_SIZE);

        long aggregateIntervalMemory = 2 * REF_SIZE + OBJECT_OVERHEAD + LONG_SIZE;

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD +
                aggregatesMemory + countsMemory + LONG_SIZE + INT_SIZE + aggregateIntervalMemory;


        return deepMemorySize;
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
            // minutes
            AggregateInterval.of(1, ChronoUnit.MINUTES),
            AggregateInterval.of(2, ChronoUnit.MINUTES),
            AggregateInterval.of(3, ChronoUnit.MINUTES),
            AggregateInterval.of(4, ChronoUnit.MINUTES),
            AggregateInterval.of(5, ChronoUnit.MINUTES),
            AggregateInterval.of(6, ChronoUnit.MINUTES),
            AggregateInterval.of(10, ChronoUnit.MINUTES),
            AggregateInterval.of(12, ChronoUnit.MINUTES),
            AggregateInterval.of(15, ChronoUnit.MINUTES),
            AggregateInterval.of(20, ChronoUnit.MINUTES),
            AggregateInterval.of(30, ChronoUnit.MINUTES),

            //hours
            AggregateInterval.of(1, ChronoUnit.HOURS),
            AggregateInterval.of(2, ChronoUnit.HOURS),
            AggregateInterval.of(3, ChronoUnit.HOURS),
            AggregateInterval.of(4, ChronoUnit.HOURS),
            AggregateInterval.of(6, ChronoUnit.HOURS),
            AggregateInterval.of(8, ChronoUnit.HOURS),
            AggregateInterval.of(12, ChronoUnit.HOURS),

            //days/weeks
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
}
