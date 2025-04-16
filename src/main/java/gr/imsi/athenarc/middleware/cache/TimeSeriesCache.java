package gr.imsi.athenarc.middleware.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.IntervalTree;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.ArrayList;
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
    
    public TimeSeriesCache() {
        this.measureToIntervalTree = new HashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
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
            }
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
     * Gets all time series spans that overlap with a given time interval for a specific measure,
     * filtered by a maximum aggregation interval relative to the pixel column interval.
     * 
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param pixelColumnInterval The width of a pixel column in time units
     * @return A list of time series spans that overlap with the interval and satisfy the aggregation constraint
     */
    public List<TimeSeriesSpan> getOverlappingSpansForVisualization(int measure, TimeInterval interval, long pixelColumnInterval) {
        try {
            cacheLock.readLock().lock();
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                    // Keep only spans with an aggregate interval that is half or less than the pixel column interval
                    // to ensure at least one fully contained in every pixel column that the span fully overlaps
                    .filter(span -> pixelColumnInterval >= 2 * span.getAggregateInterval())
                    .collect(Collectors.toList());
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
    public Map<Integer, List<TimeSeriesSpan>> getFromCacheForVisualization(VisualQuery query, long pixelColumnInterval) {
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
}
