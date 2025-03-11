package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Represents the Kleene plus operator (+) - matches one or more occurrences.
 */
public class KleenePlus implements RepetitionFactor {

    // Maximum number of repetitions to try before stopping
    private final int maxRepetitions;
    
    public KleenePlus() {
        // Default to a reasonable upper limit to prevent infinite loops
        this(Integer.MAX_VALUE);
    }
    
    public KleenePlus(int maxRepetitions) {
        this.maxRepetitions = maxRepetitions;
    }

    @Override
    public boolean isValid(int count) {
        // One or more - must have at least one match
        return count >= 1;
    }

}
