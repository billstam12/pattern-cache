package gr.imsi.athenarc.visual.middleware.patterncache.query.repetition;

/**
 * Represents a range of repetitions {min,max} - matches between min and max occurrences inclusive.
 */
public class RangeRepetition implements RepetitionFactor {

    private final int minCount;
    private final int maxCount;

    public RangeRepetition(int minCount, int maxCount) {
        if (minCount < 0) {
            throw new IllegalArgumentException("Minimum count cannot be negative");
        }
        if (maxCount < minCount) {
            throw new IllegalArgumentException("Maximum count cannot be less than minimum count");
        }
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    @Override
    public boolean isValid(int count) {
        return count >= minCount && count <= maxCount;
    }

    public int getMinCount() {
        return minCount;
    }
    
    public int getMaxCount() {
        return maxCount;
    }
}
