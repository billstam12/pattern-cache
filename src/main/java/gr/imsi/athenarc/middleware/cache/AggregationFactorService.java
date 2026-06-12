package gr.imsi.athenarc.middleware.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized, thread-safe store of the per-measure aggregation factor.
 * Same α applies regardless of the path (visual / pattern) and basis
 * (pixelColumnInterval / timeUnit) the caller is operating at.
 */
public class AggregationFactorService {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationFactorService.class);
    private static final AggregationFactorService INSTANCE = new AggregationFactorService();

    private final Map<Integer, Integer> aggFactors = new ConcurrentHashMap<>();

    private AggregationFactorService() {}

    public static AggregationFactorService getInstance() {
        return INSTANCE;
    }

    /**
     * Read the persisted α for {@code measure}, or {@code fallback} if absent.
     * The fallback is not written into the map — the next refine/relax write
     * will materialise the entry.
     */
    public int getAggFactor(int measure, int fallback) {
        Integer v = aggFactors.get(measure);
        return v != null ? v : fallback;
    }

    /** Write the α for {@code measure}. */
    public void setAggFactor(int measure, int aggFactor) {
        LOG.debug("Setting aggFactor for measure {} to {}", measure, aggFactor);
        aggFactors.put(measure, aggFactor);
    }

    /** Clear all persisted entries. */
    public void clearAll() {
        aggFactors.clear();
    }
}
