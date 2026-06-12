package gr.imsi.athenarc.middleware.pattern;

/**
 * Strategy for how to advance the search position after finding a match
 * when using non-overlapping search mode.
 */
public enum AdvancementStrategy {
    /**
     * Advance to the position immediately after the end of the current match.
     * This ensures no overlapping between matches.
     */
    AFTER_MATCH_END,
    
    /**
     * Advance to the next position after the start of the current match.
     * This allows partial overlaps where matches can share some sketches.
     */
    AFTER_MATCH_START
}
