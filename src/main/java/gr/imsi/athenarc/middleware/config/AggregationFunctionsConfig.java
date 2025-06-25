package gr.imsi.athenarc.middleware.config;

import java.util.Set;

import gr.imsi.athenarc.middleware.domain.AggregationType;

/**
 * Central configuration for aggregation functions used across the application.
 */
public class AggregationFunctionsConfig {
    
    private static final Set<String> MIN_MAX_FUNCTIONS = Set.of("min", "max");
    private static final Set<String> M4INF_FUNCTIONS = Set.of("min", "max", "first", "last");
    
    /**
     * Gets the appropriate set of aggregate functions based on the specified type.
     * 
     * @param type The type of aggregation ("minmax" or "m4*")
     * @return Set of aggregation function names
     */
    public static Set<String> getAggregateFunctions(String type) {
        if ("minmax".equalsIgnoreCase(type)) {
            return MIN_MAX_FUNCTIONS;
        } else if ("m4Inf".equalsIgnoreCase(type)) {
            return M4INF_FUNCTIONS;
        } else if ("m4".equalsIgnoreCase(type)) {
            return M4INF_FUNCTIONS;
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static Set<String> getAggregateFunctions(AggregationType aggregationType) {
        if (aggregationType == AggregationType.FIRST_VALUE) {
            return Set.of("first");
        } else if (aggregationType == AggregationType.LAST_VALUE) {
            return Set.of("last");
        } else if (aggregationType == AggregationType.MAX_VALUE) {
            return Set.of("max");
        } else if (aggregationType == AggregationType.MIN_VALUE) {
            return Set.of("min");
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
        }
    }
}
