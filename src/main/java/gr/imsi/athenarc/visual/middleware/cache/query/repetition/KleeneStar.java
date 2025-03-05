package gr.imsi.athenarc.visual.middleware.cache.query.repetition;

public class KleeneStar implements RepetitionFactor {

    // * => "zero or more"

    @Override
    public boolean isCountValid(int count) {
        // zero or more is always valid
        return (count >= 0);
    }

    @Override
    public double repetitionPenalty(int count) {
        // If everything is allowed, we can define zero penalty always.
        // Or define a penalty if we prefer a certain "optimal" count for approximate matching.
        return 0.0;
    }
}
