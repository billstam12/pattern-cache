package gr.imsi.athenarc.middleware.query;


/**
 * Base interface for all query types in the system.
 */
public interface Query {
    /**
     * Get the start time of the query range.
     * @return timestamp in milliseconds
     */
    long getFrom();
    
    /**
     * Get the end time of the query range.
     * @return timestamp in milliseconds
     */
    long getTo();
    
    /**
     * Get the measure/metric identifier(s) for this query.
     * @return measure identifier(s)
     */
    int[] getMeasures();
    
    /**
     * Get the type of this query.
     * @return the query type
     */
    QueryType getType();
}
