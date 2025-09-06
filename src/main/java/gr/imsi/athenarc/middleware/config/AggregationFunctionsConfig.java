package gr.imsi.athenarc.middleware.config;

import java.util.Set;

/**
 * Central configuration for aggregation functions used across the application.
 */
public class AggregationFunctionsConfig {
    
    private static final Set<String> MIN_MAX_FUNCTIONS = Set.of("min", "max");
    private static final Set<String> M4INF_FUNCTIONS = Set.of("min", "max", "first", "last");
    private static final Set<String> FIRST_LAST_FUNCTIONS = Set.of("first", "last");
    /**
     * Gets the appropriate set of aggregate functions based on the specified type.
     * 
     * @param type The type of aggregation ("minMax" or "m4Inf" or "firstLast" or "approxOls")
     * @return Set of aggregation function names
     */
    public static Set<String> getAggregateFunctions(String type) {
        switch (type){
            case "visual":
            case "approxOls":
            case "minMax":
                return MIN_MAX_FUNCTIONS;
            case "m4Inf":
            case "m4":
                return M4INF_FUNCTIONS;
            case "firstLast":
            case "firstLastInf":
                return FIRST_LAST_FUNCTIONS;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
            
        }
    }
}
