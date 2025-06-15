package gr.imsi.athenarc.middleware.config;

import java.util.Set;

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
        } else {
            // Default to M4* functions
            return M4INF_FUNCTIONS;
        }
    }
}
