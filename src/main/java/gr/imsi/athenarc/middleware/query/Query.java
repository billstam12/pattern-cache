package gr.imsi.athenarc.middleware.query;

import gr.imsi.athenarc.middleware.domain.ViewPort;

import java.util.List;

/**
 * Base interface for all query types in the system.
 */
public interface Query {
    /**
     * Gets the starting timestamp for this query.
     * 
     * @return The from timestamp in milliseconds
     */
    long getFrom();

    /**
     * Gets the ending timestamp for this query.
     * 
     * @return The to timestamp in milliseconds
     */
    long getTo();

    /**
     * Gets the list of measure IDs included in this query.
     * 
     * @return List of measure IDs
     */
    List<Integer> getMeasures();
    
    /**
     * Gets the type of this query.
     * 
     * @return The query type
     */
    QueryType getType();
    
    /**
     * Gets the viewport for this query.
     * 
     * @return The viewport
     */
    ViewPort getViewPort();
    
    /**
     * Gets the accuracy level for this query.
     * 
     * @return The accuracy level between 0.0 and 1.0
     */
    double getAccuracy();
}
