package gr.imsi.athenarc.middleware.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized service for managing aggregation factors across the application.
 * This service maintains aggregation factors per measure and provides thread-safe
 * access for both visual and pattern query components.
 */
public class AggregationFactorService {
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregationFactorService.class);
    private static final AggregationFactorService INSTANCE = new AggregationFactorService();
    
    private final Map<Integer, Integer> aggFactors = new ConcurrentHashMap<>();
    private final int defaultAggFactor;
    
    private AggregationFactorService() {
        this.defaultAggFactor = 1; // Default aggregation factor
    }
    
    public static AggregationFactorService getInstance() {
        return INSTANCE;
    }
    
    /**
     * Initialize the service with a default aggregation factor
     */
    public void initialize(int defaultAggFactor) {
        LOG.info("Initializing AggregationFactorService with default factor: {}", defaultAggFactor);
    }
    
    /**
     * Get aggregation factor for a specific measure
     */
    public int getAggFactor(int measure) {
        return aggFactors.getOrDefault(measure, defaultAggFactor);
    }
    
    /**
     * Set aggregation factor for a specific measure
     */
    public void setAggFactor(int measure, int aggFactor) {
        LOG.debug("Setting aggFactor for measure {} to {}", measure, aggFactor);
        aggFactors.put(measure, aggFactor);
    }
    
    /**
     * Update aggregation factor by doubling it (used when error threshold is exceeded)
     */
    public void updateAggFactor(int measure) {
        int prevAggFactor = getAggFactor(measure);
        int newAggFactor = (int) Math.floor(prevAggFactor * 2.0);
        setAggFactor(measure, newAggFactor);
        LOG.debug("Updated aggFactor for measure {} from {} to {}", measure, prevAggFactor, newAggFactor);
    }
    
    /**
     * Get all current aggregation factors
     */
    public Map<Integer, Integer> getAllAggFactors() {
        return new ConcurrentHashMap<>(aggFactors);
    }
    
    /**
     * Initialize aggregation factor for a measure 
     */
    public void initializeAggFactor(int measure, int initialValue) {
        aggFactors.put(measure, initialValue);
    }
    
    /**
     * Reset aggregation factor for a measure to default
     */
    public void resetAggFactor(int measure) {
        aggFactors.put(measure, defaultAggFactor);
    }
    
    /**
     * Clear all aggregation factors
     */
    public void clearAll() {
        aggFactors.clear();
    }
}
