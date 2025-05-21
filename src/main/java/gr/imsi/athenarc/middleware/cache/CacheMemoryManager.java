package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages memory usage of the TimeSeriesCache by tracking user access patterns
 * and evicting spans based on configurable policies.
 */
public class CacheMemoryManager {
    private static final Logger LOG = LoggerFactory.getLogger(CacheMemoryManager.class);
    
    // Maps measure IDs to their last access time
    private final Map<Integer, Long> measureLastAccessTime = new ConcurrentHashMap<>();
    
    // Maps measure IDs to their access count
    private final Map<Integer, AtomicLong> measureAccessCount = new ConcurrentHashMap<>();
    
    // Tracks the most recently accessed time ranges for visual exploration
    private final Deque<TimeRangeContext> recentTimeRanges = new LinkedList<>();
    private final int maxRecentRanges;
    
    // Memory thresholds and configuration
    private final long maxMemoryBytes;
    private final double memoryUsageThreshold;
    private final TimeSeriesCache cache;
    
    // Track current cache memory usage
    private final AtomicLong currentCacheMemoryBytes = new AtomicLong(0);
    
    // Background memory monitoring
    private final ScheduledExecutorService scheduler;
    private final boolean monitorMemoryPeriodically;
    
    /**
     * Internal class to track time range context including the aggregation level
     * that was used when exploring that range.
     */
    private static class TimeRangeContext {
        private final TimeRange range;
        private final AggregateInterval preferredInterval;
        private final long lastAccessed;
        
        public TimeRangeContext(TimeRange range, AggregateInterval preferredInterval) {
            this.range = range;
            this.preferredInterval = preferredInterval;
            this.lastAccessed = System.currentTimeMillis();
        }
        
        public TimeRange getRange() {
            return range;
        }
        
        public AggregateInterval getPreferredInterval() {
            return preferredInterval;
        }
        
        public long getLastAccessed() {
            return lastAccessed;
        }
        
        public boolean overlaps(TimeRangeContext other) {
            return this.range.overlaps(other.range);
        }
        
        public boolean isNearby(TimeRangeContext other) {
            return Math.abs(this.range.getFrom() - other.range.getFrom()) < 1000 || 
                   Math.abs(this.range.getTo() - other.range.getTo()) < 1000;
        }
    }
    
    /**
     * Creates a memory manager for the cache with default settings.
     * 
     * @param cache The cache to manage
     * @param maxMemoryBytes Maximum memory to use
     */
    public CacheMemoryManager(TimeSeriesCache cache, long maxMemoryBytes) {
        this(cache, maxMemoryBytes, 0.8, 10, false);
    }
    
    /**
     * Creates a memory manager with custom settings.
     * 
     * @param cache The cache to manage
     * @param maxMemoryBytes Maximum memory to use
     * @param memoryUsageThreshold Threshold (0.0-1.0) at which to start eviction
     * @param maxRecentRanges Number of recent time ranges to track
     * @param monitorMemoryPeriodically Whether to periodically check memory usage
     */
    public CacheMemoryManager(
            TimeSeriesCache cache,
            long maxMemoryBytes,
            double memoryUsageThreshold,
            int maxRecentRanges,
            boolean monitorMemoryPeriodically) {
        
        this.cache = cache;
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryUsageThreshold = memoryUsageThreshold;
        this.maxRecentRanges = maxRecentRanges;
        this.monitorMemoryPeriodically = monitorMemoryPeriodically;
        
        if (monitorMemoryPeriodically) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "Cache-Memory-Manager");
                thread.setDaemon(true);
                return thread;
            });
            
            // Schedule periodic memory check every 30 seconds
            scheduler.scheduleWithFixedDelay(
                this::checkMemoryAndEvict, 
                30, 30, TimeUnit.SECONDS
            );
        } else {
            scheduler = null;
        }
        
        // Initialize memory tracking by calculating current usage
        updateCurrentMemoryUsage();
        
        LOG.info("Cache memory manager initialized with {} MB max memory, {}% threshold, current usage: {} MB",
                maxMemoryBytes / (1024*1024), memoryUsageThreshold * 100, 
                currentCacheMemoryBytes.get() / (1024*1024));
    }
    
    /**
     * Records a measure access to track usage patterns.
     * 
     * @param measure The measure ID that was accessed
     */
    public void recordMeasureAccess(int measure) {
        measureLastAccessTime.put(measure, System.currentTimeMillis());
        measureAccessCount.computeIfAbsent(measure, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Records a time range access to track user's area of exploration.
     * 
     * @param timeRange The time range that was accessed
     * @param preferredInterval The preferred aggregation interval for this range
     */
    public void recordTimeRangeAccess(TimeRange timeRange, AggregateInterval preferredInterval) {
        TimeRangeContext context = new TimeRangeContext(timeRange, preferredInterval);
        
        synchronized (recentTimeRanges) {
            // Remove existing overlapping or nearby ranges
            recentTimeRanges.removeIf(existing -> 
                existing.overlaps(context) || existing.isNearby(context)
            );
            
            // Add the new range to the front
            recentTimeRanges.addFirst(context);
            
            // Trim the list to the maximum size
            while (recentTimeRanges.size() > maxRecentRanges) {
                recentTimeRanges.removeLast();
            }
        }
        
        LOG.debug("Recorded access for time range {} to {} with preferred interval {}",
                timeRange.getFrom(), timeRange.getTo(), preferredInterval);
    }
    
    /**
     * Records a time range access to track user's area of exploration.
     * This is a backward-compatible overload that uses a default preferred interval.
     * 
     * @param timeRange The time range that was accessed
     */
    public void recordTimeRangeAccess(TimeRange timeRange) {
        // Use a default "unknown" aggregation interval when not specified
        recordTimeRangeAccess(timeRange, AggregateInterval.DEFAULT);
    }
    
    /**
     * Track memory allocation for a newly added span.
     * 
     * @param span The span that was added to the cache
     */
    public void trackSpanAddition(TimeSeriesSpan span) {
        if (span != null) {
            long spanSize = span.calculateDeepMemorySize();
            currentCacheMemoryBytes.addAndGet(spanSize);
            
            LOG.debug("Added span for measure {}, size: {} bytes, total cache: {} MB", 
                    span.getMeasure(), spanSize, currentCacheMemoryBytes.get() / (1024*1024));
            
            // Check if we need to evict after adding this span
            if (currentCacheMemoryBytes.get() > memoryUsageThreshold * maxMemoryBytes) {
                checkMemoryAndEvict();
            }
        }
    }
    
    /**
     * Track memory deallocation for a removed span.
     * 
     * @param span The span that was removed from the cache
     */
    public void trackSpanRemoval(TimeSeriesSpan span) {
        if (span != null) {
            long spanSize = span.calculateDeepMemorySize();
            currentCacheMemoryBytes.addAndGet(-spanSize);
            
            LOG.debug("Removed span for measure {}, freed: {} bytes, total cache: {} MB", 
                    span.getMeasure(), spanSize, currentCacheMemoryBytes.get() / (1024*1024));
        }
    }
    
    /**
     * Update the current memory usage by scanning all spans in the cache.
     */
    public void updateCurrentMemoryUsage() {
        long total = 0;
        
        for (Integer measure : cache.getAllMeasures()) {
            List<TimeSeriesSpan> spans = cache.getAllSpansForMeasure(measure);
            for (TimeSeriesSpan span : spans) {
                total += span.calculateDeepMemorySize();
            }
        }
        
        currentCacheMemoryBytes.set(total);
        LOG.debug("Updated current memory usage: {} MB", total / (1024*1024));
    }
    
    /**
     * Explicitly trigger memory check and potential eviction.
     * 
     * @return The number of spans evicted
     */
    public int checkMemoryAndEvict() {
        long usedMemory = currentCacheMemoryBytes.get();
        
        LOG.debug("Cache memory usage: {} MB / {} MB ({}%)", 
                usedMemory / (1024*1024), 
                maxMemoryBytes / (1024*1024),
                (usedMemory * 100) / maxMemoryBytes);
        
        if (usedMemory > memoryUsageThreshold * maxMemoryBytes) {
            return evictLowPrioritySpans();
        }
        
        return 0;
    }
    
    /**
     * Evict spans based on access patterns and distance from current exploration.
     * 
     * @return The number of spans evicted
     */
    private int evictLowPrioritySpans() {
        LOG.info("Memory threshold reached, evicting low-priority spans: {} MB / {} MB ({}%)",
                currentCacheMemoryBytes.get() / (1024*1024),
                maxMemoryBytes / (1024*1024),
                (currentCacheMemoryBytes.get() * 100) / maxMemoryBytes);
        
        int totalEvicted = 0;
        
        // Step 1: Get least accessed measures
        List<Integer> measures = new ArrayList<>(measureLastAccessTime.keySet());
        measures.sort(Comparator.comparing(measureLastAccessTime::get));
        
        // Process the 50% least recently accessed measures
        int measuresToProcess = Math.max(1, measures.size() / 2);
        for (int i = 0; i < measuresToProcess && i < measures.size(); i++) {
            int measure = measures.get(i);
            totalEvicted += evictForMeasure(measure);
            
            // If we've evicted enough, stop
            if (checkMemoryImprovement()) {
                break;
            }
        }
        
        LOG.info("Cache eviction complete, removed {} spans, current memory: {} MB", 
                totalEvicted, currentCacheMemoryBytes.get() / (1024*1024));
        return totalEvicted;
    }
    
    /**
     * Evict spans for a specific measure, focusing on those far from current exploration
     * or those with unsuitable aggregation levels.
     * 
     * @param measure The measure ID
     * @return The number of spans evicted
     */
    private int evictForMeasure(int measure) {
        int evicted = 0;
        int rolledUp = 0;
        
        // Get current exploration regions
        List<TimeRangeContext> explorationContexts = getCurrentExplorationContexts();
        
        // Find all spans for this measure
        List<TimeSeriesSpan> allSpans = cache.getAllSpansForMeasure(measure);
        if (allSpans.isEmpty()) {
            return 0;
        }
        
        // Calculate the average span size to use for distance metrics
        long totalSpanLength = 0;
        for (TimeSeriesSpan span : allSpans) {
            totalSpanLength += (span.getTo() - span.getFrom());
        }
        long averageSpanLength = totalSpanLength / allSpans.size();
        
        // Group spans into categories
        List<TimeSeriesSpan> farAwaySpans = new ArrayList<>();
        List<TimeSeriesSpan> overlappingInappropriateGranularity = new ArrayList<>();
        List<TimeSeriesSpan> overlappingAppropriateGranularity = new ArrayList<>();
        
        for (TimeSeriesSpan span : allSpans) {
            boolean overlapsAny = false;
            boolean hasAppropriateGranularity = false;
            
            for (TimeRangeContext context : explorationContexts) {
                TimeRange range = context.getRange();
                AggregateInterval preferredInterval = context.getPreferredInterval();
                
                if (span.getFrom() < range.getTo() && span.getTo() > range.getFrom()) {
                    overlapsAny = true;
                    
                    // Check if this span's aggregation interval is appropriate for the context
                    if (isAppropriateGranularity(span.getAggregateInterval(), preferredInterval)) {
                        hasAppropriateGranularity = true;
                        break;  // No need to check other contexts if we found a match
                    }
                }
            }
            
            if (overlapsAny) {
                if (hasAppropriateGranularity) {
                    overlappingAppropriateGranularity.add(span);
                } else {
                    overlappingInappropriateGranularity.add(span);
                }
            } else {
                farAwaySpans.add(span);
            }
        }
        
        LOG.debug("Measure {}: {} spans far away, {} overlapping with inappropriate granularity, {} overlapping with appropriate granularity",
                measure, farAwaySpans.size(), overlappingInappropriateGranularity.size(), overlappingAppropriateGranularity.size());
        
        // Step 1: First roll up spans that are far from current exploration
        if (!farAwaySpans.isEmpty()) {
            // Sort by distance (furthest first)
            Set<TimeInterval> preserveIntervals = getCurrentExplorationIntervals();
            farAwaySpans.sort((s1, s2) -> {
                long d1 = minDistanceToAnyInterval(s1, preserveIntervals, averageSpanLength);
                long d2 = minDistanceToAnyInterval(s2, preserveIntervals, averageSpanLength);
                return Long.compare(d2, d1); // Descending order
            });
            
            // Try to roll up far away spans first
            int spanCount = Math.min(farAwaySpans.size(), Math.max(1, farAwaySpans.size() * 3 / 4));
            for (int i = 0; i < spanCount; i++) {
                TimeSeriesSpan span = farAwaySpans.get(i);
                
                // Only AggregateTimeSeriesSpan can be rolled up
                if (span instanceof AggregateTimeSeriesSpan) {
                    AggregateTimeSeriesSpan aggSpan = (AggregateTimeSeriesSpan) span;
                    
                    // Try to roll up to twice the current interval
                    AggregateInterval targetInterval = getNextLargerInterval(aggSpan.getAggregateInterval());
                    
                    if (targetInterval != null) {
                        // Check if rolling up would save enough memory
                        long memorySavings = aggSpan.estimateRollUpMemorySavings(targetInterval);
                        
                        if (memorySavings > 0 && memorySavings > aggSpan.calculateDeepMemorySize() * 0.3) {
                            // Remove the original span
                            if (cache.removeSpan(span)) {
                                // Create and add the rolled up span
                                AggregateTimeSeriesSpan rolledUpSpan = aggSpan.rollUp(targetInterval);
                                if (rolledUpSpan != null) {
                                    LOG.debug("Rolled up far away span for measure {}: {} to {} from interval {} to {}, saving {} bytes", 
                                            measure, span.getFromDate(), span.getToDate(), 
                                            aggSpan.getAggregateInterval(), targetInterval, memorySavings);
                                            
                                    // Track removal of the original span
                                    trackSpanRemoval(span);
                                    
                                    // Add the new rolled up span
                                    cache.addToCache(rolledUpSpan);
                                    
                                    rolledUp++;
                                    continue; // Skip to next span
                                }
                            }
                        }
                    }
                }
                
                // If we couldn't roll up or it wouldn't help enough, evict
                LOG.debug("Evicting far away span for measure {}: {} to {} with interval {}, size: {} bytes", 
                        measure, span.getFromDate(), span.getToDate(), span.getAggregateInterval(), 
                        span.calculateDeepMemorySize());
                
                if (cache.removeSpan(span)) {
                    trackSpanRemoval(span);
                    evicted++;
                }
            }
            
            // Check if we've freed enough memory
            if (checkMemoryImprovement()) {
                LOG.info("Memory improved after processing far away spans: evicted {}, rolled up {}", evicted, rolledUp);
                return evicted;
            }
        }
        
        // Step 2: Then try to roll up spans that overlap but have inappropriate granularity 
        if (!overlappingInappropriateGranularity.isEmpty()) {
            // Sort by how inappropriate the granularity is (most inappropriate first)
            overlappingInappropriateGranularity.sort((s1, s2) -> {
                double d1 = getGranularityInappropriatenessScore(s1, explorationContexts);
                double d2 = getGranularityInappropriatenessScore(s2, explorationContexts);
                return Double.compare(d2, d1); // Descending order
            });
            
            // Process up to 50% of the inappropriate granularity spans
            int spanCount = Math.min(overlappingInappropriateGranularity.size(), 
                                Math.max(1, overlappingInappropriateGranularity.size() / 2));
                                
            for (int i = 0; i < spanCount; i++) {
                TimeSeriesSpan span = overlappingInappropriateGranularity.get(i);
                
                // See if we can roll up the span to a more appropriate interval
                if (span instanceof AggregateTimeSeriesSpan) {
                    AggregateTimeSeriesSpan aggSpan = (AggregateTimeSeriesSpan) span;
                    
                    // Find the most appropriate target interval for this span
                    AggregateInterval bestTargetInterval = findBestRollUpInterval(aggSpan, explorationContexts);
                    
                    if (bestTargetInterval != null) {
                        // Check if rolling up would save enough memory
                        long memorySavings = aggSpan.estimateRollUpMemorySavings(bestTargetInterval);
                        
                        if (memorySavings > 0 && memorySavings > aggSpan.calculateDeepMemorySize() * 0.2) {
                            // Remove the original span
                            if (cache.removeSpan(span)) {
                                // Create and add the rolled up span
                                AggregateTimeSeriesSpan rolledUpSpan = aggSpan.rollUp(bestTargetInterval);
                                if (rolledUpSpan != null) {
                                    LOG.debug("Rolled up overlap span for measure {}: {} to {} from interval {} to {}, saving {} bytes", 
                                            measure, span.getFromDate(), span.getToDate(), 
                                            aggSpan.getAggregateInterval(), bestTargetInterval, memorySavings);
                                            
                                    // Track removal of the original span
                                    trackSpanRemoval(span);
                                    
                                    // Add the new rolled up span
                                    cache.addToCache(rolledUpSpan);
                                    
                                    rolledUp++;
                                    continue; // Skip to next span
                                }
                            }
                        }
                    }
                }
                
                // If we couldn't roll up or it wouldn't help enough, evict
                LOG.debug("Evicting inappropriate granularity span for measure {}: {} to {} with interval {}, size: {} bytes", 
                        measure, span.getFromDate(), span.getToDate(), span.getAggregateInterval(),
                        span.calculateDeepMemorySize());
                
                if (cache.removeSpan(span)) {
                    trackSpanRemoval(span);
                    evicted++;
                }
            }
        }
        
        LOG.info("Memory management for measure {}: evicted {}, rolled up {}", measure, evicted, rolledUp);
        return evicted;
    }
    
    /**
     * Get the next larger standard aggregation interval.
     * 
     * @param current The current aggregation interval
     * @return The next larger interval, or null if already at maximum
     */
    private AggregateInterval getNextLargerInterval(AggregateInterval current) {
        long currentMs = current.toDuration().toMillis();
        
        // Standard intervals in ascending order of duration
        List<AggregateInterval> standardIntervals = DateTimeUtil.CALENDAR_INTERVALS;
        
        // Find the next larger interval
        for (int i = 0; i < standardIntervals.size(); i++) {
            long intervalMs = standardIntervals.get(i).toDuration().toMillis();
            if (intervalMs > currentMs) {
                return standardIntervals.get(i);
            }
        }
        
        // If we're already at the largest standard interval
        return null;
    }
    
    /**
     * Finds the best target interval to roll up to based on exploration contexts.
     * 
     * @param span The span to roll up
     * @param contexts The current exploration contexts
     * @return The best target interval, or null if none is suitable
     */
    private AggregateInterval findBestRollUpInterval(AggregateTimeSeriesSpan span, List<TimeRangeContext> contexts) {
        AggregateInterval currentInterval = span.getAggregateInterval();
        long currentMs = currentInterval.toDuration().toMillis();
        
        // First, try to find the most appropriate interval based on exploration contexts
        for (TimeRangeContext context : contexts) {
            // Skip contexts that don't overlap
            if (span.getFrom() >= context.getRange().getTo() || span.getTo() <= context.getRange().getFrom()) {
                continue;
            }
            
            // Skip contexts with default interval
            if (context.getPreferredInterval() == AggregateInterval.DEFAULT) {
                continue;
            }
            
            long preferredMs = context.getPreferredInterval().toDuration().toMillis();
            
            // If our interval is already too large, we need to find a compromise
            if (currentMs > preferredMs * 2) {
                return null; // Can't roll up if already too large
            }
            
            // Otherwise, try to find an interval that's close to but not smaller than the preferred
            AggregateInterval nextLarger = getNextLargerInterval(currentInterval);
            if (nextLarger != null) {
                long nextMs = nextLarger.toDuration().toMillis();
                
                // If rolling up would still maintain compatibility with the exploration
                if (nextMs <= preferredMs * 4) {
                    return nextLarger;
                }
            }
        }
        
        // If we couldn't find a context-specific interval, just use the next larger one
        return getNextLargerInterval(currentInterval);
    }
    
    /**
     * Checks whether a span's aggregation interval is appropriate for the preferred interval
     * from a time range context.
     * 
     * @param spanInterval The span's aggregation interval
     * @param preferredInterval The preferred interval for the context
     * @return true if the span's interval is appropriate
     */
    private boolean isAppropriateGranularity(AggregateInterval spanInterval, AggregateInterval preferredInterval) {
        // If the preferred interval is DEFAULT (unknown), accept any interval
        if (preferredInterval == AggregateInterval.DEFAULT) {
            return true;
        }
        
        // Get the millisecond durations
        long spanMs = spanInterval.toDuration().toMillis();
        long preferredMs = preferredInterval.toDuration().toMillis();
        
        // The span's interval should be no more than 2x smaller and no more than 4x larger
        // than the preferred interval for optimal visualization
        return spanMs <= preferredMs * 2 && spanMs >= preferredMs / 4;
    }
    
    /**
     * Calculate a score representing how inappropriate a span's granularity is
     * compared to the exploration contexts.
     * 
     * @param span The span to evaluate
     * @param contexts The exploration contexts
     * @return A score where higher means more inappropriate
     */
    private double getGranularityInappropriatenessScore(TimeSeriesSpan span, List<TimeRangeContext> contexts) {
        double worstScore = 0;
        
        for (TimeRangeContext context : contexts) {
            // Skip contexts that don't overlap with this span
            if (span.getFrom() >= context.getRange().getTo() || span.getTo() <= context.getRange().getFrom()) {
                continue;
            }
            
            // If the preferred interval is DEFAULT, skip this context
            if (context.getPreferredInterval() == AggregateInterval.DEFAULT) {
                continue;
            }
            
            // Calculate ratio between span interval and preferred interval
            long spanMs = span.getAggregateInterval().toDuration().toMillis();
            long preferredMs = context.getPreferredInterval().toDuration().toMillis();
            
            double ratio;
            if (spanMs > preferredMs) {
                // If span is too coarse (ratio > 1)
                ratio = spanMs / (double) preferredMs;
            } else {
                // If span is too fine (ratio < 1)
                ratio = preferredMs / (double) spanMs;
            }
            
            // Adjust by recency - more recent contexts matter more
            long ageMs = System.currentTimeMillis() - context.getLastAccessed();
            double recencyFactor = 1.0 - Math.min(1.0, ageMs / (double)(1000 * 60 * 5)); // 5 minutes max age
            
            double score = ratio * recencyFactor;
            worstScore = Math.max(worstScore, score);
        }
        
        return worstScore;
    }
    
    /**
     * Calculate the minimum distance from a span to any of the current exploration intervals.
     * 
     * @param span The span to check
     * @param intervals Current exploration intervals
     * @param normalizer Value to normalize distances with
     * @return A normalized distance value
     */
    private long minDistanceToAnyInterval(TimeSeriesSpan span, Set<TimeInterval> intervals, long normalizer) {
        if (intervals.isEmpty()) {
            return Long.MAX_VALUE;
        }
        
        long minDistance = Long.MAX_VALUE;
        for (TimeInterval interval : intervals) {
            // If they overlap, distance is 0
            if (span.getFrom() < interval.getTo() && span.getTo() > interval.getFrom()) {
                return 0;
            }
            
            // Calculate the distance between the intervals
            long distance;
            if (span.getTo() <= interval.getFrom()) {
                distance = interval.getFrom() - span.getTo();
            } else {
                distance = span.getFrom() - interval.getTo();
            }
            
            minDistance = Math.min(minDistance, distance);
        }
        
        // Normalize by the average span length
        return normalizer > 0 ? minDistance / normalizer : minDistance;
    }
    
    /**
     * Get the time intervals the user is currently exploring.
     * 
     * @return Set of time intervals to prioritize
     */
    private Set<TimeInterval> getCurrentExplorationIntervals() {
        Set<TimeInterval> intervals = new HashSet<>();
        
        synchronized (recentTimeRanges) {
            for (TimeRangeContext context : recentTimeRanges) {
                intervals.add(context.getRange());
            }
            
            // If no recent ranges, create a very large interval
            if (intervals.isEmpty()) {
                // Default to the entire possible time range
                intervals.add(new TimeRange(0, Long.MAX_VALUE));
            }
        }
        
        return intervals;
    }
    
    /**
     * Get the current exploration contexts including aggregation preferences.
     * 
     * @return List of exploration contexts
     */
    private List<TimeRangeContext> getCurrentExplorationContexts() {
        List<TimeRangeContext> contexts = new ArrayList<>();
        
        synchronized (recentTimeRanges) {
            contexts.addAll(recentTimeRanges);
            
            // If no recent ranges, create a default context
            if (contexts.isEmpty()) {
                contexts.add(new TimeRangeContext(
                    new TimeRange(0, Long.MAX_VALUE), AggregateInterval.DEFAULT));
            }
        }
        
        return contexts;
    }
    
    /**
     * Check if memory has improved enough after eviction.
     * 
     * @return true if we're now under the threshold
     */
    private boolean checkMemoryImprovement() {
        long usedMemory = currentCacheMemoryBytes.get();
        return usedMemory < memoryUsageThreshold * 0.8 * maxMemoryBytes;
    }
    
    /**
     * Get the current cache memory usage in bytes.
     * 
     * @return Current memory usage in bytes
     */
    public long getCurrentMemoryBytes() {
        return currentCacheMemoryBytes.get();
    }
    
    /**
     * Get the maximum allowed memory in bytes.
     * 
     * @return Maximum memory in bytes
     */
    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }
    
    /**
     * Get the current memory utilization as a percentage.
     * 
     * @return Memory utilization percentage (0-100)
     */
    public double getMemoryUtilizationPercentage() {
        return (currentCacheMemoryBytes.get() * 100.0) / maxMemoryBytes;
    }
    
    /**
     * Shutdown the memory manager, stopping any background tasks.
     */
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while shutting down memory manager", e);
            }
        }
    }
}
