package gr.imsi.athenarc.visual.middleware.cache.query.repetition;
public class RangeRepetition implements RepetitionFactor {

    private final int minCount;
    private final int maxCount;

    public RangeRepetition(int minCount, int maxCount) {
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    @Override
    public boolean isCountValid(int count) {
        return (count >= minCount && count <= maxCount);
    }

    @Override
    public double repetitionPenalty(int count) {
        // zero penalty if in [minCount..maxCount],
        // else penalty is how far outside that range we are.
        if (count >= minCount && count <= maxCount) {
            return 0.0;
        } else if (count < minCount) {
            return (minCount - count);
        } else { // count > maxCount
            return (count - maxCount);
        }
    }
}
