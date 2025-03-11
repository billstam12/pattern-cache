package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Requires exactly N consecutive matches of a segment.
 */
public class Exactly implements RepetitionFactor {
    private final int count;
    
    public Exactly(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }
        this.count = count;
    }
    
    @Override
    public boolean isValid(int matchCount) {
        return matchCount == count;
    }
    
    public int getCount() {
        return count;
    }
}
