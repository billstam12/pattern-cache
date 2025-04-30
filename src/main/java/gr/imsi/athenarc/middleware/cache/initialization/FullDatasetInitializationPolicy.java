package gr.imsi.athenarc.middleware.cache.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.manager.visual.VisualQueryManager;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;

import java.util.ArrayList;
import java.util.Map;

/**
 * A cache initialization policy that loads the entire dataset range 
 * with a specific aggregation level for each measure.
 */
public class FullDatasetInitializationPolicy implements CacheInitializationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(FullDatasetInitializationPolicy.class);
    
    private final Map<Integer, AggregateInterval> aggregateIntervalsByMeasure;
    
    /**
     * Creates a policy that initializes the entire dataset with specified aggregation intervals.
     * 
     * @param aggregateIntervalsByMeasure Map of measure IDs to their aggregation intervals
     */
    public FullDatasetInitializationPolicy(Map<Integer, AggregateInterval> aggregateIntervalsByMeasure) {
        this.aggregateIntervalsByMeasure = aggregateIntervalsByMeasure;
    }
    
    @Override
    public void initialize(TimeSeriesCache cache, VisualQueryManager visualQueryManager) {
        LOG.info("Initializing cache with full dataset");
        AbstractDataset dataset = visualQueryManager.getDataSource().getDataset();
        
        // Create a single visual query that handles all measures with their respective intervals
        VisualQuery query = VisualQuery.builder()
            .withMeasures(new ArrayList<>(aggregateIntervalsByMeasure.keySet()))
            .withMeasureAggregateIntervals(aggregateIntervalsByMeasure)
            .withTimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo())
            .withViewPort(1000, 500)  // Default viewport
            .withAccuracy(0.95)        // Use default accuracy
            .build();
        
        try {
            visualQueryManager.executeQuery(query);
            LOG.info("Successfully pre-loaded all measures with their specified intervals");
        } catch (Exception e) {
            LOG.error("Failed to pre-load measures: {}", e.getMessage(), e);
        }
        
        LOG.info("Cache initialization completed");
    }
    
    @Override
    public String getDescription() {
        return "Full dataset initialization with custom aggregation intervals per measure";
    }
}
