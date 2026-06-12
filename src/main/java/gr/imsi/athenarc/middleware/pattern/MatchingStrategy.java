package gr.imsi.athenarc.middleware.pattern;

/**
 * Defines strategies for handling found matches.
 */
public enum MatchingStrategy {
    /**
     * Allow all overlapping matches to be returned.
     */
    ALL,
    
    /**
     * Prevent some matches using a selection strategy.
     */
    SELECTION;
}
