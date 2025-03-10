package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

public class KleenePlus implements RepetitionFactor {

    // + => "one or more"

    @Override
    public boolean isCountValid(int count) {
        // one or more => must be at least 1
        return (count >= 1);
    }

    @Override
    public double repetitionPenalty(int count) {
        // zero penalty if count >= 1, else big penalty for 0
        // In approximate logic, if count=0, maybe penalty=9999, or we can do count-based.
        return (count >= 1) ? 0.0 : 9999.0;
    }
}
