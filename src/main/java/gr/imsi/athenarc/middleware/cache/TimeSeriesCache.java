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
import java.util.TreeMap;
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
     * Gets time series spans for a visual query on a measure. Aggregated spans must satisfy
     * {@code 2 * aggregateInterval <= pixelColumnInterval} so error computation has at least
     * two data points per pixel column; raw spans are always included so their pixel-perfect
     * data still drives the no-error optimization in {@link gr.imsi.athenarc.middleware.visual.DataProcessor}.
     *
     * <p>Among admissible aggregated spans, only the coarsest-per-region cover is returned —
     * finer admissible spans whose region is already claimed by a coarser one are dropped so
     * downstream processing doesn't redundantly iterate them.
     *
     * @param measure The measure ID
     * @param interval The time interval to check for overlap
     * @param pixelColumnInterval The width of a pixel column in time units
     * @return raw spans overlapping the interval plus the coarsest-per-region aggregated cover
     */
    public List<TimeSeriesSpan> getOverlappingSpansForVisualization(int measure, TimeInterval interval, AggregateInterval pixelColumnInterval) {
        try {
            cacheLock.readLock().lock();

            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()), pixelColumnInterval);
            }

            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }

            long pcMs = pixelColumnInterval.toDuration().toMillis();
            List<TimeSeriesSpan> rawSpans = new ArrayList<>();
            java.util.Iterator<TimeSeriesSpan> it = tree.overlappers(interval);
            while (it.hasNext()) {
                TimeSeriesSpan span = it.next();
                if (span instanceof RawTimeSeriesSpan) {
                    rawSpans.add(span);
                }
            }

            // 2x guard: visual error computation needs at least two data points per
            // pixel column, so admissible spans satisfy 2*aggInterval <= pixelColumn.
            AggregateInterval maxAggInterval = AggregateInterval.fromMillis(pcMs / 2);
            // Visual collects raw spans separately above; tell the cover to skip them
            // so the concatenated result isn't duplicated.
            List<CoveredSpan> covered = getCoarsestPerRegionCoverageInternal(tree, interval, maxAggInterval, false);
            java.util.LinkedHashSet<TimeSeriesSpan> coarsest = new java.util.LinkedHashSet<>(covered.size());
            for (CoveredSpan cs : covered) {
                coarsest.add(cs.getSpan());
            }
            List<TimeSeriesSpan> result = new ArrayList<>(rawSpans.size() + coarsest.size());
            result.addAll(rawSpans);
            result.addAll(coarsest);
            return result;
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
     * Tree-level coarsest-per-region cover. Pure algorithm; assumes the caller holds
     * the read lock and has already done memory-manager bookkeeping.
     */
    private static List<CoveredSpan> getCoarsestPerRegionCoverageInternal(
            IntervalTree<TimeSeriesSpan> tree, TimeInterval interval, AggregateInterval maxAggInterval,
            boolean acceptRaw) {
        return getCoarsestPerRegionCoverageInternal(tree, interval, maxAggInterval, acceptRaw, null);
    }

    private static List<CoveredSpan> getCoarsestPerRegionCoverageInternal(
            IntervalTree<TimeSeriesSpan> tree, TimeInterval interval, AggregateInterval maxAggInterval,
            boolean acceptRaw, AggregateInterval requireDivisorOf) {
        final long maxAggMs = maxAggInterval.toDuration().toMillis();
        final long qFrom = interval.getFrom();
        final long qTo = interval.getTo();
        if (qFrom >= qTo) {
            return new ArrayList<>();
        }

        // (1) Collect admissible spans within the query range.
        List<TimeSeriesSpan> admissible = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(tree.overlappers(interval), 0), false)
                .filter(s -> s.getAggregateInterval().toDuration().toMillis() <= maxAggMs)
                .filter(s -> acceptRaw || !(s instanceof RawTimeSeriesSpan))
                .filter(s -> requireDivisorOf == null
                        || DateTimeUtil.isCompatibleWithTarget(s.getAggregateInterval(), requireDivisorOf))
                .collect(Collectors.toList());
        if (admissible.isEmpty()) {
            return new ArrayList<>();
        }

        // (2) Sort coarsest-first; ties broken by start time so output is deterministic.
        admissible.sort((a, b) -> {
            long am = a.getAggregateInterval().toDuration().toMillis();
            long bm = b.getAggregateInterval().toDuration().toMillis();
            int cmp = Long.compare(bm, am);
            return cmp != 0 ? cmp : Long.compare(a.getFrom(), b.getFrom());
        });

        // (3) Greedy claim. unclaimed: TreeMap<startTime, endTime> of still-uncovered
        // sub-ranges. floorEntry/ceilingEntry give O(log K) lookups.
        TreeMap<Long, Long> unclaimed = new TreeMap<>();
        unclaimed.put(qFrom, qTo);
        // Emissions before final sort: triples (ownedFrom, ownedTo, idx-into-admissible).
        List<long[]> emissions = new ArrayList<>(admissible.size());

        for (int idx = 0; idx < admissible.size(); idx++) {
            if (unclaimed.isEmpty()) break;
            TimeSeriesSpan s = admissible.get(idx);
            long sFrom = Math.max(qFrom, s.getFrom());
            long sTo = Math.min(qTo, s.getTo());
            if (sFrom >= sTo) continue;

            // First unclaimed entry that could overlap [sFrom, sTo): the floor entry
            // covers it iff its end is past sFrom; otherwise hop to the ceiling.
            Map.Entry<Long, Long> entry = unclaimed.floorEntry(sFrom);
            if (entry == null || entry.getValue() <= sFrom) {
                entry = unclaimed.ceilingEntry(sFrom);
            }
            while (entry != null && entry.getKey() < sTo) {
                long uFrom = entry.getKey();
                long uTo = entry.getValue();
                long claimStart = Math.max(uFrom, sFrom);
                long claimEnd = Math.min(uTo, sTo);
                emissions.add(new long[]{claimStart, claimEnd, idx});
                // Replace [uFrom, uTo) with the (possibly empty) side-pieces.
                unclaimed.remove(uFrom);
                if (uFrom < claimStart) unclaimed.put(uFrom, claimStart);
                if (claimEnd < uTo) unclaimed.put(claimEnd, uTo);
                entry = unclaimed.ceilingEntry(claimEnd);
            }
        }

        // (4) Sort emissions by owned start. No same-span merge needed (see javadoc).
        emissions.sort((a, b) -> Long.compare(a[0], b[0]));
        List<CoveredSpan> result = new ArrayList<>(emissions.size());
        for (long[] em : emissions) {
            result.add(new CoveredSpan(admissible.get((int) em[2]), em[0], em[1]));
        }
        return result;
    }

       /**
     * Strict-alignment overload: when {@code requireDivisorOf} is non-null, only
     * admit spans whose {@code aggregateInterval} aggregates cleanly into
     * {@code requireDivisorOf} (via {@link DateTimeUtil#isCompatibleWithTarget}).
     * Pattern-side strict callers pass the new query's {@code timeUnit} so cached
     * sub-buckets are guaranteed to tile cleanly inside each outer bucket — no
     * straddlers at the outer-bucket grid, no silent partial coverage downstream.
     * Relaxed callers pass {@code null} for current behaviour (admit any span
     * with {@code aggregateInterval ≤ maxAggInterval}).
     */
    public List<CoveredSpan> getCoarsestPerRegionCoverage(int measure, TimeInterval interval,
                                                          AggregateInterval maxAggInterval,
                                                          boolean acceptRaw,
                                                          AggregateInterval requireDivisorOf) {
        try {
            cacheLock.readLock().lock();
            if (memoryManager != null) {
                memoryManager.recordMeasureAccess(measure);
                memoryManager.recordTimeRangeAccess(new TimeRange(interval.getFrom(), interval.getTo()), maxAggInterval);
            }
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.get(measure);
            if (tree == null) {
                return new ArrayList<>();
            }
            return getCoarsestPerRegionCoverageInternal(tree, interval, maxAggInterval, acceptRaw, requireDivisorOf);
        } finally {
            cacheLock.readLock().unlock();
        }
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
            IntervalTree<TimeSeriesSpan> tree = measureToIntervalTree.remove(measure);
            if (tree != null && memoryManager != null) {
                tree.forEach(memoryManager::trackSpanRemoval);
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
            if (memoryManager != null) {
                for (IntervalTree<TimeSeriesSpan> tree : measureToIntervalTree.values()) {
                    tree.forEach(memoryManager::trackSpanRemoval);
                }
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
