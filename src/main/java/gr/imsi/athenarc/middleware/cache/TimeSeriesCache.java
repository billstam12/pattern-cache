package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.IntervalTree;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A unified cache mechanism for storing time series metadata and processed data
 * that can be used by different types of queries.
 */
public class TimeSeriesCache {
    
    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesCache.class);
    
    private final Map<Integer, IntervalTree<TimeSeriesSpan>> measureToIntervalTree;
    private final ReadWriteLock cacheLock;
    private CacheMemoryManager memoryManager;
    
    public TimeSeriesCache() {
        this.measureToIntervalTree = new HashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
    }
    
    /**
     * Sets the memory manager for this cache.
     * 
     * @param memoryManager The memory manager to use
     */
    public void setMemoryManager(CacheMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        
        // Update memory usage with all current spans
        if (memoryManager != null) {
            memoryManager.updateCurrentMemoryUsage();
        }
    }
    
    /**
     * Initializes the cache for specific measures.
     * 
     * @param measures The list of measures to initialize
     */
    public void initializeForMeasures(List<Integer> measures) {
        try {
            cacheLock.writeLock().lock();
            for (Integer measure : measures) {
                if (!measureToIntervalTree.containsKey(measure)) {
                    measureToIntervalTree.put(measure, new IntervalTree<>());
                }
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Adds a time series span to the cache.
     * 
     * @param span The time series span to add
     */
    public void addToCache(TimeSeriesSpan span) {
        try {
            cacheLock.writeLock().lock();
            IntervalTree<TimeSeriesSpan> tree = getOrCreateIntervalTree(span.getMeasure());
            tree.insert(span);
            
            // Track memory usage if memory manager is configured
            if (memoryManager != null) {
                memoryManager.trackSpanAddition(span);
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Adds multiple time series spans to the cache.
     * 
     * @param spans The list of time series spans to add
     */
    public void addToCache(List<TimeSeriesSpan> spans) {
        try {
            cacheLock.writeLock().lock();
            for (TimeSeriesSpan span : spans) {
                IntervalTree<TimeSeriesSpan> tree = getOrCreateIntervalTree(span.getMeasure());
                tree.insert(span);
                
                // Track memory usage if memory manager is configured
                if (memoryManager != null) {
                    memoryManager.trackSpanAddition(span);
                }
            }
        } catch (Exception e) {
            LOG.error("Error adding spans to cache", e);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    
    /**
     * Gets all time series spans that overlap with a given time interval for a specific measure.
     * 
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @return A list of time series spans that overlap with the interval
     */
    public List<TimeSeriesSpan> getOverlappingSpans(int measure, TimeInterval interval) {
        try {
            cacheLock.readLock().lock();
            
            // Track access pattern if memory manager is configured
            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()));
            }
            
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    .collect(Collectors.toList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Gets all time series spans that overlap with a given time interval for a specific measure
     * and have exactly the same aggregation interval as the target interval.
     * 
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param targetInterval The target aggregation interval
     * @return A list of time series spans that overlap with the interval and have exactly the same aggregation interval
     */
    public List<TimeSeriesSpan> getExactCompatibleSpans(int measure, TimeInterval interval, AggregateInterval targetInterval) {
        try {
            cacheLock.readLock().lock();
            
            // Track access pattern if memory manager is configured
            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()), targetInterval);
            }
            
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            
            // Filter spans by compatible intervals
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    .filter(span -> {
                        // Check if the span's aggregate interval is compatible
                        return span.getAggregateInterval() == targetInterval;     
                    })
                    .collect(Collectors.toList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Gets all time series spans that overlap with a given time interval for a specific measure
     * and have compatible aggregation intervals that can be used to compute the target interval.
     * 
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param targetInterval The target aggregation interval
     * @return A list of time series spans that overlap with the interval and have compatible aggregation
     */
    public List<TimeSeriesSpan> getCompatibleSpans(int measure, TimeInterval interval, AggregateInterval targetInterval) {
        try {
            cacheLock.readLock().lock();
            
            // Track access pattern if memory manager is configured
            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()), targetInterval);
            }
            
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            
            // Filter spans by compatible intervals
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    .filter(span -> {
                        // Check if the span's aggregate interval is compatible
                        return DateTimeUtil.isCompatibleWithTarget(span.getAggregateInterval(), targetInterval);     
                    })
                    .collect(Collectors.toList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Gets all time series spans that overlap with a given time interval for a specific measure,
     * filtered by a maximum aggregation interval relative to the pixel column interval.
     * 
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param pixelColumnInterval The width of a pixel column in time units
     * @return A list of time series spans that overlap with the interval and satisfy the aggregation constraint
     */
    public List<TimeSeriesSpan> getOverlappingSpansForVisualization(int measure, TimeInterval interval, AggregateInterval pixelColumnInterval) {
        try {
            cacheLock.readLock().lock();
            
            // Track access pattern if memory manager is configured
            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()), pixelColumnInterval);
            }
            
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    // Keep only spans with an aggregate interval that is half or less than the pixel column interval
                    // to ensure at least one fully contained in every pixel column that the span fully overlaps
                    .filter(span -> pixelColumnInterval.toDuration().toMillis() >= 2 * span.getAggregateInterval().toDuration().toMillis())
                    .collect(Collectors.toList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }


    /**
     * Gets the spans with the most coverage for a compatible aggregate interval.
     * For the given interval and target aggregate interval, finds all compatible spans,
     * groups them by their aggregate interval, and returns the group that covers the largest
     * portion of the interval.
     *
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param targetInterval The target aggregation interval
     * @return A list of time series spans with the most coverage and the same compatible aggregate interval
     */
    public List<TimeSeriesSpan> getMaxCoverageCompatibleSpans(int measure, TimeInterval interval, AggregateInterval targetInterval) {
        try {
            cacheLock.readLock().lock();
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }

            // Step 1: Find all compatible spans
            List<TimeSeriesSpan> compatibleSpans = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    .filter(span -> DateTimeUtil.isCompatibleWithTarget(span.getAggregateInterval(), targetInterval))
                    .collect(Collectors.toList());

            // Step 2: Group by aggregate interval
            Map<AggregateInterval, List<TimeSeriesSpan>> byAgg = compatibleSpans.stream()
                    .collect(Collectors.groupingBy(TimeSeriesSpan::getAggregateInterval));

            // Step 3: For each group, calculate total coverage
            AggregateInterval bestAgg = null;
            long maxCoverage = -1;
            for (Map.Entry<AggregateInterval, List<TimeSeriesSpan>> entry : byAgg.entrySet()) {
                long coverage = 0;
                for (TimeSeriesSpan span : entry.getValue()) {
                    coverage += span.percentage(interval)   ;
                    // long overlapStart = Math.max(span.getFrom(), interval.getFrom());
                    // long overlapEnd = Math.min(span.getTo(), interval.getTo());
                    // if (overlapStart < overlapEnd) {
                    //     coverage += (overlapEnd - overlapStart);
                    // }
                }
                if (coverage > maxCoverage) {
                    maxCoverage = coverage;
                    bestAgg = entry.getKey();
                }
            }

            // Step 4: Return the spans from the group with max coverage
            if (bestAgg != null) {
                return byAgg.get(bestAgg);
            } else {
                return new ArrayList<>();
            }
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Gets all overlapping spans for each measure in a visual query.
     * 
     * @param query The visual query
     * @param pixelColumnInterval The width of a pixel column in time units
     * @return A map of measure IDs to lists of time series spans
     */
    public Map<Integer, List<TimeSeriesSpan>> getFromCacheForVisualization(VisualQuery query, AggregateInterval pixelColumnInterval) {
        return query.getMeasures().stream().collect(Collectors.toMap(
                m -> m,
                m -> getOverlappingSpansForVisualization(m, query, pixelColumnInterval)
        ));
    }
    
    /**
     * Gets or creates an interval tree for a specific measure.
     * 
     * @param measure The measure ID
     * @return The interval tree for the measure
     */
    private IntervalTree<TimeSeriesSpan> getOrCreateIntervalTree(int measure) {
        IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
        if (tree == null) {
            tree = new IntervalTree<>();
            measureToIntervalTree.put(measure, tree);
        }
        return tree;
    }
    
    /**
     * Gets the interval tree for a specific measure if it exists.
     * 
     * @param measure The measure ID
     * @return The interval tree for the measure, or null if it doesn't exist
     */
    public IntervalTree<TimeSeriesSpan> getIntervalTree(int measure) {
        try {
            cacheLock.readLock().lock();
            return measureToIntervalTree.get(measure);
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Clears the cache for a specific measure.
     * 
     * @param measure The measure ID
     */
    public void clearCache(int measure) {
        try {
            cacheLock.writeLock().lock();
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree != null) {
                // tree.clear();
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Clears the entire cache.
     */
    public void clearCache() {
        try {
            cacheLock.writeLock().lock();
            for (IntervalTree<TimeSeriesSpan> tree : measureToIntervalTree.values()) {
                // tree.clear();
            }
            measureToIntervalTree.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Removes a specific span from the cache.
     * 
     * @param span The span to remove
     * @return true if the span was removed, false otherwise
     */
    public boolean removeSpan(TimeSeriesSpan span) {
        try {
            cacheLock.writeLock().lock();
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(span.getMeasure());
            if (tree != null) {
                boolean removed = tree.delete(span);
                return removed;
            }
            return false;
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets all spans for a specific measure (used for memory management).
     * 
     * @param measure The measure ID
     * @return A list of all spans for the measure
     */
    public List<TimeSeriesSpan> getAllSpansForMeasure(int measure) {
        try {
            cacheLock.readLock().lock();
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            
            List<TimeSeriesSpan> allSpans = new ArrayList<>();
            tree.forEach(allSpans::add);
            return allSpans;
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Gets all measure IDs in the cache.
     *
     * @return Set of all measure IDs
     */
    public Set<Integer> getAllMeasures() {
        try {
            cacheLock.readLock().lock();
            return new HashSet<>(measureToIntervalTree.keySet());
        } finally {
            cacheLock.readLock().unlock();
        }
    }
}
