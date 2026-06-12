package gr.imsi.athenarc.middleware.pattern;

/**
 * Defines strategies for selecting matches when multiple matches are found
 * at the same starting position.
 */
public enum MatchSelectionStrategy {    
    /**
     * Select the shortest match (minimum number of sketches consumed).
     */
    SHORTEST,
    
    /**
     * Select the longest match (maximum number of sketches consumed).
     */
    LONGEST;
}
