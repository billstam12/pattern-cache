package gr.imsi.athenarc.middleware.domain;

/**
 * Utility class for working with Stats implementations.
 */
public class StatsUtils {
    
    /**
     * Safely gets the minimum timestamp from a Stats object.
     * 
     * @param stats The Stats object
     * @param defaultTimestamp The default timestamp to return if not available
     * @return The minimum timestamp or the default if not available
     */
    public static long getMinTimestampSafely(Stats stats, long defaultTimestamp) {
        try {
            return stats.getMinTimestamp();
        } catch (UnsupportedOperationException e) {
            return defaultTimestamp;
        }
    }
    
    /**
     * Safely gets the maximum timestamp from a Stats object.
     * 
     * @param stats The Stats object
     * @param defaultTimestamp The default timestamp to return if not available
     * @return The maximum timestamp or the default if not available
     */
    public static long getMaxTimestampSafely(Stats stats, long defaultTimestamp) {
        try {
            return stats.getMaxTimestamp();
        } catch (UnsupportedOperationException e) {
            return defaultTimestamp;
        }
    }
    
    /**
     * Safely gets the first timestamp from a Stats object.
     * 
     * @param stats The Stats object
     * @param defaultTimestamp The default timestamp to return if not available
     * @return The first timestamp or the default if not available
     */
    public static long getFirstTimestampSafely(Stats stats, long defaultTimestamp) {
        try {
            return stats.getFirstTimestamp();
        } catch (UnsupportedOperationException e) {
            return defaultTimestamp;
        }
    }
    
    /**
     * Safely gets the last timestamp from a Stats object.
     * 
     * @param stats The Stats object
     * @param defaultTimestamp The default timestamp to return if not available
     * @return The last timestamp or the default if not available
     */
    public static long getLastTimestampSafely(Stats stats, long defaultTimestamp) {
        try {
            return stats.getLastTimestamp();
        } catch (UnsupportedOperationException e) {
            return defaultTimestamp;
        }
    }
    
    /**
     * Checks if a Stats implementation supports timestamp information.
     * 
     * @param stats The Stats object to check
     * @return true if timestamps are supported, false otherwise
     */
    public static boolean supportsTimestamps(Stats stats) {
        try {
            stats.getMinTimestamp();
            stats.getMaxTimestamp();
            stats.getFirstTimestamp();
            stats.getLastTimestamp();
            return true;
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }
}
