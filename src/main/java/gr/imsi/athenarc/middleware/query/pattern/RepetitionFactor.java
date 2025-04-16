package gr.imsi.athenarc.middleware.query.pattern;

/**
 * Represents how many times a pattern node should be repeated.
 */
public class RepetitionFactor {
    private final int minRepetitions;
    private final int maxRepetitions;
    
    /**
     * Creates a repetition factor with exact number of repetitions
     * 
     * @param exactRepetitions The exact number of repetitions
     */
    public static RepetitionFactor exactly(int exactRepetitions) {
        return new RepetitionFactor(exactRepetitions, exactRepetitions);
    }
    
    /**
     * Creates a repetition factor with a range of repetitions
     * 
     * @param min Minimum repetitions (inclusive)
     * @param max Maximum repetitions (inclusive)
     */
    public static RepetitionFactor range(int min, int max) {
        return new RepetitionFactor(min, max);
    }
    
    /**
     * Creates a repetition factor with at least min repetitions
     * 
     * @param min Minimum repetitions (inclusive)
     */
    public static RepetitionFactor atLeast(int min) {
        return new RepetitionFactor(min, Integer.MAX_VALUE);
    }
    
    /**
     * Creates a repetition factor with at most max repetitions
     * 
     * @param max Maximum repetitions (inclusive)
     */
    public static RepetitionFactor atMost(int max) {
        return new RepetitionFactor(1, max);
    }
    
    /**
     * Creates a repetition factor with zero or more repetitions
     */
    public static RepetitionFactor zeroOrMore() {
        return new RepetitionFactor(0, Integer.MAX_VALUE);
    }
    
    /**
     * Creates a repetition factor with one or more repetitions
     */
    public static RepetitionFactor oneOrMore() {
        return new RepetitionFactor(1, Integer.MAX_VALUE);
    }
    
    /**
     * Creates a repetition factor with zero or one repetition
     */
    public static RepetitionFactor zeroOrOne() {
        return new RepetitionFactor(0, 1);
    }
    
    private RepetitionFactor(int minRepetitions, int maxRepetitions) {
        if (minRepetitions < 0) {
            throw new IllegalArgumentException("Minimum repetitions cannot be negative");
        }
        if (maxRepetitions < minRepetitions) {
            throw new IllegalArgumentException("Maximum repetitions cannot be less than minimum repetitions");
        }
        this.minRepetitions = minRepetitions;
        this.maxRepetitions = maxRepetitions;
    }
    
    public int getMinRepetitions() {
        return minRepetitions;
    }
    
    public int getMaxRepetitions() {
        return maxRepetitions;
    }
    
    @Override
    public String toString() {
        if (minRepetitions == maxRepetitions) {
            return "{" + minRepetitions + "}";
        } else if (maxRepetitions == Integer.MAX_VALUE) {
            if (minRepetitions == 0) {
                return "{*}";
            } else if (minRepetitions == 1) {
                return "{+}";
            } else {
                return "{" + minRepetitions + ",*}";
            }
        } else if (minRepetitions == 0 && maxRepetitions == 1) {
            return "{?}";
        } else {
            return "{" + minRepetitions + "," + maxRepetitions + "}";
        }
    }
}
