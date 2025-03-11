package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Interface representing how many times a segment can be repeated.
 */
public interface RepetitionFactor {
    
    /**
     * Determines if the given number of matches satisfies this repetition factor.
     * 
     * @param count The number of consecutive matches
     * @return true if the count satisfies the repetition factor, false otherwise
     */
    boolean isValid(int count);

}
