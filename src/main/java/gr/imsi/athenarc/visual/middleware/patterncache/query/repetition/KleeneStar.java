package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Represents the Kleene star operator (*) - matches zero or more occurrences.
 */
public class KleeneStar implements RepetitionFactor {

    // Maximum number of repetitions to try before stopping
    private final int maxRepetitions;
    
    public KleeneStar() {
        // Default to a reasonable upper limit to prevent infinite loops
        this(Integer.MAX_VALUE);
    }
    
    public KleeneStar(int maxRepetitions) {
        this.maxRepetitions = maxRepetitions;
    }

    @Override
    public boolean isValid(int count) {
        // Zero or more is always valid
        return count >= 0;
    }
}
