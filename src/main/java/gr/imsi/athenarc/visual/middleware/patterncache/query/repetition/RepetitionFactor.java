package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

public interface RepetitionFactor {

    /**
     * Checks if the given count of segments "count" satisfies 
     * this repetition factor exactly (for strict matching).
     * 
     * @param count The number of times a segment spec is repeated
     * @return true if this repetition factor is satisfied, false otherwise
     */
    boolean isCountValid(int count);

    /**
     * Returns a "penalty" based on how much the given count deviates
     * from this repetition factor, for approximate matching.
     * 
     * If you do not need approximate matching, you could return 
     * 0.0 when isCountValid is true, and a large number (e.g. 9999) otherwise.
     * 
     * @param count The number of times a segment spec is repeated
     * @return a non-negative penalty (0 means perfectly matched)
     */
    double repetitionPenalty(int count);
}
