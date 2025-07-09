package gr.imsi.athenarc.middleware.query.pattern;

/**
 * Represents filtering criteria for values in a pattern.
 * Primarily focused on angle filtering with bounds from -90 to 90 degrees.
 */
public class ValueFilter {
    // Whether to consider any value
    private final boolean valueAny;
    
    private final float minDegree; // Minimum degree value (inclusive)
    private final float maxDegree; // Maximum degree value (inclusive)
    
    /**
     * Creates a value filter that accepts any value.
     */
    public static ValueFilter any() {
        return new ValueFilter(true, 0, 0);
    }
    
    /**
     * Creates a value filter for increasing trends (positive degrees)
     */
    public static ValueFilter increasing() {
        return new ValueFilter(false, 10f, 80f);
    }

    /**
     * Creates a value filter for moderate increasing trends
     */
    public static ValueFilter moderateIncrease(){
        return new ValueFilter(false, 10f, 45f);
    }

    /**
     * Creates a value filter for large increasing trends
     */
    public static ValueFilter largeIncrease(){
        return new ValueFilter(false, 45f, 80f);
    }
    
    /**
     * Creates a value filter for decreasing trends (negative degrees)
     */
    public static ValueFilter decreasing() {
        return new ValueFilter(false, -80f, -10f);
    }

     /**
     * Creates a value filter for moderate decreasing trends
     */
    public static ValueFilter moderateDecrease(){
        return new ValueFilter(false, -45f, -10f);
    }

     /**
     * Creates a value filter for large decreasing trends
     */
    public static ValueFilter largeDecrease(){
        return new ValueFilter(false, -80f, -45f);
    }
    
    /**
     * Creates a value filter for stable trends (degrees near zero)
     */
    public static ValueFilter stable() {
        return new ValueFilter(false, -10f, 10f);
    }
    
    /**
     * Creates a custom value filter with specific degree bounds
     * 
     * @param minDegree Minimum degree value (between -1 and 1)
     * @param maxDegree Maximum degree value (between -1 and 1)
     */
    public static ValueFilter custom(float minDegree, float maxDegree) {
        if (minDegree < -1 || minDegree > 1 || maxDegree < -1 || maxDegree > 1 || minDegree > maxDegree) {
            throw new IllegalArgumentException("Degree values must be between -1 and 1, and min must be <= max");
        }
        return new ValueFilter(false, minDegree, maxDegree);
    }
    
    private ValueFilter(boolean valueAny, float minDegree, float maxDegree) {
        this.valueAny = valueAny;
        this.minDegree = minDegree;
        this.maxDegree = maxDegree;
    }
    
    public boolean isValueAny() {
        return valueAny;
    }
    
    public float getMinDegree() {
        return minDegree;
    }
    
    public float getMaxDegree() {
        return maxDegree;
    }
    
    @Override
    public String toString() {
        if (valueAny) {
            return "ValueFilter: ANY";
        } else {
            return String.format("ValueFilter: Degree[%.2f, %.2f]", minDegree, maxDegree);
        }
    }
}
