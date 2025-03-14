package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Kleene star is "zero or more", plus is "one or more".
 * You might unify them with a "minCount" only, setting 
 * star => minCount=0, plus => minCount=1, and no maxCount.
 */
public class KleeneRepetition implements RepetitionFactor {

    private final int minCount; // 0 for '*', 1 for '+'

    public KleeneRepetition(int minCount) {
        this.minCount = minCount;
    }
    
    @Override
    public boolean isValid(int count) {
        return (count >= minCount);
    }
}
