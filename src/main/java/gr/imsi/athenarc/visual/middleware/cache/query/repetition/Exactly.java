package gr.imsi.athenarc.visual.middleware.cache.query.repetition;

public class Exactly implements RepetitionFactor {

    private final int k;

    public Exactly(int k) {
        this.k = k;
    }

    @Override
    public boolean isCountValid(int count) {
        return (count == k);
    }

    @Override
    public double repetitionPenalty(int count) {
        // Example approximate logic:
        //  zero penalty if count == k,
        //  otherwise penalty grows with the absolute difference.
        return Math.abs(count - k);
    }
}
