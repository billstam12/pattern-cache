package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.manager.pattern.Sketch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A unified cache mechanism for storing time series metadata and processed data
 * that can be used by different types of queries.
 */
public class TimeSeriesCache {
    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesCache.class);
    
    // Cache for sketches organized by measure and time range
    private Map<Integer, Map<String, List<Sketch>>> sketchCache = new HashMap<>();
    
    // Cache for aggregated data points organized by measure and time range
    private Map<Integer, Map<String, AggregatedDataPoints>> dataPointsCache = new HashMap<>();
    
    /**
     * Store sketches in the cache.
     *
     * @param measure the measure identifier
     * @param from start time
     * @param to end time
     * @param chronoUnit the time unit for the sketches
     * @param sketches the sketches to store
     */
    public void storeSketchesForMeasure(int measure, long from, long to, ChronoUnit chronoUnit, List<Sketch> sketches) {
        String cacheKey = generateCacheKey(from, to, chronoUnit);
        
        Map<String, List<Sketch>> measureCache = sketchCache.computeIfAbsent(measure, k -> new HashMap<>());
        measureCache.put(cacheKey, sketches);
        
        LOG.debug("Stored {} sketches for measure {} with key {}", sketches.size(), measure, cacheKey);
    }
    
    /**
     * Retrieve sketches from the cache.
     *
     * @param measure the measure identifier
     * @param from start time
     * @param to end time
     * @param chronoUnit the time unit for the sketches
     * @return list of sketches or null if not found
     */
    public List<Sketch> getSketchesForMeasure(int measure, long from, long to, ChronoUnit chronoUnit) {
        String cacheKey = generateCacheKey(from, to, chronoUnit);
        
        Map<String, List<Sketch>> measureCache = sketchCache.get(measure);
        if (measureCache == null) {
            return null;
        }
        
        List<Sketch> cachedSketches = measureCache.get(cacheKey);
        if (cachedSketches != null) {
            LOG.debug("Cache hit: Found {} sketches for measure {} with key {}", 
                    cachedSketches.size(), measure, cacheKey);
        } else {
            LOG.debug("Cache miss: No sketches found for measure {} with key {}", measure, cacheKey);
        }
        
        return cachedSketches;
    }
    
    /**
     * Store aggregated data points in the cache.
     *
     * @param measure the measure identifier
     * @param from start time
     * @param to end time
     * @param dataPoints the aggregated data points to store
     */
    public void storeDataPointsForMeasure(int measure, long from, long to, AggregatedDataPoints dataPoints) {
        String cacheKey = generateCacheKey(from, to, null);
        
        Map<String, AggregatedDataPoints> measureCache = dataPointsCache.computeIfAbsent(measure, k -> new HashMap<>());
        measureCache.put(cacheKey, dataPoints);
        
        LOG.debug("Stored data points for measure {} with key {}", measure, cacheKey);
    }
    
    /**
     * Retrieve aggregated data points from the cache.
     *
     * @param measure the measure identifier
     * @param from start time
     * @param to end time
     * @return aggregated data points or null if not found
     */
    public AggregatedDataPoints getDataPointsForMeasure(int measure, long from, long to) {
        String cacheKey = generateCacheKey(from, to, null);
        
        Map<String, AggregatedDataPoints> measureCache = dataPointsCache.get(measure);
        if (measureCache == null) {
            return null;
        }
        
        AggregatedDataPoints cachedDataPoints = measureCache.get(cacheKey);
        if (cachedDataPoints != null) {
            LOG.debug("Cache hit: Found data points for measure {} with key {}", measure, cacheKey);
        } else {
            LOG.debug("Cache miss: No data points found for measure {} with key {}", measure, cacheKey);
        }
        
        return cachedDataPoints;
    }
    
    /**
     * Clear all cached data.
     */
    public void clearCache() {
        sketchCache.clear();
        dataPointsCache.clear();
        LOG.info("Cache cleared");
    }
    
    /**
     * Generate a cache key based on time range and time unit.
     */
    private String generateCacheKey(long from, long to, ChronoUnit chronoUnit) {
        if (chronoUnit == null) {
            return from + "_" + to;
        } else {
            return from + "_" + to + "_" + chronoUnit.name();
        }
    }
}
