package gr.imsi.athenarc.middleware.query.pattern;

/**
 * Represents filtering criteria for values in a pattern.
 * Primarily focused on slope filtering with bounds from -1 to 1.
 */
public class ValueFilter {
    // Whether to consider any value
    private final boolean valueAny;
    
    // Slope filters range from -1 (strong decrease) to 1 (strong increase)
    private final float minSlope; // Minimum slope value (inclusive)
    private final float maxSlope; // Maximum slope value (inclusive)
    
    /**
     * Creates a value filter that accepts any value.
     */
    public static ValueFilter any() {
        return new ValueFilter(true, 0, 0);
    }
    
    /**
     * Creates a value filter for increasing trends (positive slopes)
     */
    public static ValueFilter increasing() {
        return new ValueFilter(false, 0.2f, 1.0f);
    }
    
    /**
     * Creates a value filter for decreasing trends (negative slopes)
     */
    public static ValueFilter decreasing() {
        return new ValueFilter(false, -1.0f, -0.2f);
    }
    
    /**
     * Creates a value filter for stable trends (slopes near zero)
     */
    public static ValueFilter stable() {
        return new ValueFilter(false, -0.2f, 0.2f);
    }
    
    /**
     * Creates a custom value filter with specific slope bounds
     * 
     * @param minSlope Minimum slope value (between -1 and 1)
     * @param maxSlope Maximum slope value (between -1 and 1)
     */
    public static ValueFilter custom(float minSlope, float maxSlope) {
        if (minSlope < -1 || minSlope > 1 || maxSlope < -1 || maxSlope > 1 || minSlope > maxSlope) {
            throw new IllegalArgumentException("Slope values must be between -1 and 1, and min must be <= max");
        }
        return new ValueFilter(false, minSlope, maxSlope);
    }
    
    private ValueFilter(boolean valueAny, float minSlope, float maxSlope) {
        this.valueAny = valueAny;
        this.minSlope = minSlope;
        this.maxSlope = maxSlope;
    }
    
    public boolean isValueAny() {
        return valueAny;
    }
    
    public float getMinSlope() {
        return minSlope;
    }
    
    public float getMaxSlope() {
        return maxSlope;
    }
    
    @Override
    public String toString() {
        if (valueAny) {
            return "ValueFilter: ANY";
        } else {
            return String.format("ValueFilter: Slope[%.2f, %.2f]", minSlope, maxSlope);
        }
    }
}
