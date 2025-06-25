package gr.imsi.athenarc.middleware.domain;


/** Defines the different aggregation types that can be used when querying sketches **/
public enum AggregationType {
    LAST_VALUE,   // Use the last value from each interval
    FIRST_VALUE,  // Use the first value from each interval
    MIN_VALUE,    // Use the minimum value from each interval
    MAX_VALUE,    // Use the maximum value from each interval
    OLS,
}
