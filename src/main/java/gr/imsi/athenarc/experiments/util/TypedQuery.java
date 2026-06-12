package gr.imsi.athenarc.experiments.util;

import gr.imsi.athenarc.middleware.query.Query;

/**
 * A container class that holds a Query along with its operation type and pattern ID (if applicable).
 */
public class TypedQuery {
    private final Query query;
    private final UserOpType userOpType;
    private final int patternId;

    /**
     * Create a TypedQuery with query and operation type.
     *
     * @param query The query object
     * @param userOpType The user operation type
     * @param patternId The pattern ID (if applicable, -1 if not a pattern query)
     */
    public TypedQuery(Query query, UserOpType userOpType, int patternId) {
        this.query = query;
        this.userOpType = userOpType;
        this.patternId = patternId;
    }

    /**
     * Get the query object.
     *
     * @return The query object
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Get the user operation type.
     *
     * @return The user operation type
     */
    public UserOpType getUserOpType() {
        return userOpType;
    }

    /**
     * Get the pattern ID (if applicable).
     *
     * @return The pattern ID or -1 if not a pattern query
     */
    public int getPatternId() {
        return patternId;
    }
}
