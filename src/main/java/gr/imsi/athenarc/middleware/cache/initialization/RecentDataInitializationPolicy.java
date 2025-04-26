package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.time.Duration;
import java.util.Map;

/**
 * A cache initialization policy that loads only the most recent data
 * with specified aggregation intervals for each measure.
 */
public class RecentDataInitializationPolicy implements CacheInitializationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(RecentDataInitializationPolicy.class);
    
    private final Duration lookbackPeriod;
    private final Map<Integer, AggregateInterval> aggregateIntervalsByMeasure;
    
    /**
     * Creates a policy that initializes recent data with specified aggregation intervals.
     * 
     * @param lookbackPeriod How far back in time to load data
     * @param aggregateIntervalsByMeasure Map of measure IDs to their aggregation intervals
     */
    public RecentDataInitializationPolicy(Duration lookbackPeriod, Map<Integer, AggregateInterval> aggregateIntervalsByMeasure) {
        this.lookbackPeriod = lookbackPeriod;
        this.aggregateIntervalsByMeasure = aggregateIntervalsByMeasure;
    }
    
    @Override
    public void initialize(TimeSeriesCache cache, VisualQueryManager visualQueryManager) {
        LOG.info("Initializing cache with recent data for the last {}", lookbackPeriod);
        AbstractDataset dataset = visualQueryManager.getDataSource().getDataset();
        
        // Calculate the start timestamp based on lookback duration
        long endTimestamp = dataset.getTimeRange().getTo();
        long startTimestamp = endTimestamp - lookbackPeriod.toMillis();
        
        // Ensure start time is not before dataset start
        startTimestamp = Math.max(startTimestamp, dataset.getTimeRange().getFrom());
        
        LOG.info("Loading data from {} to {}", startTimestamp, endTimestamp);
        
        // Create a single query with all measures and their respective intervals
        VisualQuery query = VisualQuery.builder()
            .withMeasures(dataset.getMeasures())
            .withTimeRange(startTimestamp, endTimestamp)
            .withViewPort(1000, 500)  // Default viewport
            .withAccuracy(1.0)        // Use highest accuracy
            .withMeasureAggregateIntervals(aggregateIntervalsByMeasure)
            .build();
        
        try {
            visualQueryManager.executeQuery(query);
            LOG.info("Successfully pre-loaded recent data for all measures");
        } catch (Exception e) {
            LOG.error("Failed to pre-load measures: {}", e.getMessage(), e);
        }
        
        LOG.info("Recent data initialization completed");
    }
    
    @Override
    public String getDescription() {
        return String.format("Recent data initialization for the last %s with custom aggregation intervals", lookbackPeriod);
    }
}
