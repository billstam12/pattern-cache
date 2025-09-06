package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.CacheManager;
import gr.imsi.athenarc.middleware.cache.CacheUtils;
import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final String method; // Method to use for data retrieval (e.g., "M4Inf", "M4", etc.)
    
    /**
     * Creates a memory-bounded initialization policy with full configuration.
     * 
     * @param maxMemoryBytes Maximum memory in bytes to utilize for cache
     * @param memoryUtilizationTarget Target memory utilization (0.0-1.0)
     * @param lookbackPeriod How far back in time to load data (null for full dataset)     */
    public MemoryBoundedInitializationPolicy(
            long maxMemoryBytes, 
            double memoryUtilizationTarget,
            String method) {
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryUtilizationTarget = memoryUtilizationTarget;
        this.method = method;

        if (memoryUtilizationTarget <= 0.0 || memoryUtilizationTarget > 1.0) {
            throw new IllegalArgumentException("Memory utilization target must be between 0.0 and 1.0");
        }
    }
    
    @Override
    public void initialize(CacheManager cacheManager) {
        initialize(cacheManager, null);
    }
    
    /**
     * Initialize the cache with specific measures.
     * 
     * @param cache The time series cache to initialize
     * @param measures List of measures to initialize (null means use all measures)
     */
    public void initialize(CacheManager cacheManager, List<Integer> measures) {
        LOG.info("Initializing cache with recent data for the dataset using memory-bounded approach (limit: {} bytes, utilization: {}%)", 
                maxMemoryBytes, memoryUtilizationTarget * 100);
        
        DataSource dataSource = cacheManager.getDataSource();
        AbstractDataset dataset = dataSource.getDataset();
        TimeSeriesCache cache = cacheManager.getCache();


        // Determine time range based on mode (full or recent data)
        long endTimestamp = dataset.getTimeRange().getTo();
        long startTimestamp = dataset.getTimeRange().getFrom();        
    
        long datasetTimeRange = endTimestamp - startTimestamp;

        // Use specific measures if provided, otherwise use all dataset measures
        List<Integer> measureList = measures != null ? measures : dataset.getMeasures();
        int measureCount = measureList.size();
        
        // Calculate per-measure memory budget
        long targetMemoryBytes = (long) (maxMemoryBytes * memoryUtilizationTarget);
        long perMeasureBytes = targetMemoryBytes / measureCount;
        
        LOG.info("Selected time range spans {} ms with {} measures", datasetTimeRange, measureCount);
        LOG.info("Memory budget per measure: {} bytes", perMeasureBytes);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();

        // Determine appropriate aggregation intervals
        Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure = calculateOptimalAggregationIntervals(
            measureList, perMeasureBytes, datasetTimeRange);
    
        for (int measure : measureList){
            List<TimeInterval> missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(startTimestamp, endTimestamp));
            missingIntervalsPerMeasure.put(measure, missingIntervals);
        }
        
        String method = cacheManager.getVisualQueryManager().getMethod();

        Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure = 
                DateTimeUtil.alignIntervalsToTimeUnitBoundary(missingIntervalsPerMeasure, aggregateIntervalsPerMeasure);
                                       
        // Create spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = CacheUtils.fetchTimeSeriesSpansForInitialization(dataSource, startTimestamp, endTimestamp, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, method);

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
       return String.format("Memory-bounded recent data initialization for the whole dataset (limit: %d bytes, target: %.0f%%)", 
            maxMemoryBytes, memoryUtilizationTarget * 100);
    }


    private long calculateBytesPerDataPoint() {
    // Memory usage of long in a 64-bit JVM
    final int LONG_SIZE = 8;
    
    long aggSize = 0;
    switch(method) {
        case "m4Inf":
            aggSize = 5; // min, max, first, last, count
            break;
        case "m4":
            aggSize = 9; // min, max, first, last values + 4 timestamps + count
            break;
        case "visual":
        case "approxOls":
        case "minMax":
            aggSize = 3; // min, max, count
            break;
        case "ols":
            aggSize = 5; // sum_x, sum_y, sum_xy, sum_x2, count
            break;
        default:
            throw new IllegalArgumentException("Unknown method: " + method);
    }
    
    // Each aggregate element is stored as a long (8 bytes)
    return aggSize * LONG_SIZE;
}

private Map<Integer, AggregateInterval> calculateOptimalAggregationIntervals(
    List<Integer> measures, long bytesPerMeasure, long datasetTimeRange) {
    
    Map<Integer, AggregateInterval> intervals = new HashMap<>();
    
    // Memory overhead constants
    final int OBJECT_OVERHEAD = 16;
    final int ARRAY_OVERHEAD = 24;
    final int INT_SIZE = 4;
    final int LONG_SIZE = 8;
    final int REF_SIZE = 8;
    
    // Calculate bytes per aggregated data point
    long bytesPerPoint = calculateBytesPerDataPoint();
    
    // Calculate TimeSeriesSpan object overhead
    long spanOverhead = OBJECT_OVERHEAD + // object header
                       INT_SIZE +         // measure
                       INT_SIZE +         // count  
                       REF_SIZE +         // aggregates reference
                       LONG_SIZE +        // from
                       LONG_SIZE +        // to
                       INT_SIZE +         // size
                       REF_SIZE +         // aggregateInterval reference
                       ARRAY_OVERHEAD;    // aggregates array overhead
    
    for (Integer measureId : measures) {
        // Available memory for actual data points
        long availableForData = bytesPerMeasure - spanOverhead;
        
        // Calculate how many aggregated points we can store
        long maxDataPoints = availableForData / bytesPerPoint;
        
        // Calculate minimum aggregation interval needed to fit within memory
        long minIntervalMs = datasetTimeRange / maxDataPoints;
        
        // Round up to a sensible interval
        AggregateInterval interval = DateTimeUtil.roundDownToCalendarBasedInterval(minIntervalMs);
        
        intervals.put(measureId, interval);
        
        // Calculate actual usage for logging
        long actualPoints = datasetTimeRange / interval.toDuration().toMillis();
        long actualMemory = spanOverhead + (actualPoints * bytesPerPoint);
        
        LOG.info("Measure {}: Budget {}B, overhead {}B, can store ~{} points, using interval {} (actual: {} points, {}B)", 
                measureId, bytesPerMeasure, spanOverhead, maxDataPoints, interval, actualPoints, actualMemory);
    }
    
    return intervals;
}
}
