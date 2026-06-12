package gr.imsi.athenarc.middleware.config;

import java.util.Set;

/**
 * Central configuration for aggregation functions used across the application.
 */
public class AggregationFunctionsConfig {
    
    private static final Set<String> MIN_MAX_FUNCTIONS = Set.of("min", "max");
    private static final Set<String> APPROX_OLS_FUNCTIONS = Set.of("min", "max", "count", "sum");
    private static final Set<String> M4_FUNCTIONS = Set.of("min", "max", "first", "last", "count");
    /**
     * Gets the appropriate set of aggregate functions based on the specified type.
     *
     * @param type The type of aggregation ("minMax", "approxOls", or "m4")
     * @return Set of aggregation function names
     */
    public static Set<String> getAggregateFunctions(String type) {
        switch (type){
            case "minMax":
                return MIN_MAX_FUNCTIONS;
            case "approxOls":
                return APPROX_OLS_FUNCTIONS;
            case "m4":
                return M4_FUNCTIONS;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
