package gr.imsi.athenarc.experiments.util;

import java.util.List;

import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryType;

/**
 * A wrapper class that associates a query with its type
 */
public class TypedQuery {
    
    private final Query query;
    private final UserOpType userOpType; // user operation
    
    public TypedQuery(Query query, UserOpType userOpType) {
        this.query = query;
        this.userOpType = userOpType;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public UserOpType getUserOpType() {
        return userOpType;
    }
    
    @Override
    public String toString() {
        return userOpType + ": " + query.toString();
    }
}
