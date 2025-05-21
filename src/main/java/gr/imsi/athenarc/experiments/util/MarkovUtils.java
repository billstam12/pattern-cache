package gr.imsi.athenarc.experiments.util;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;

public final class MarkovUtils {

    private static final Random RNG = new SecureRandom();   // or new Random() if crypto-strength isn’t needed

    /**
     * Picks one {@code UserOpType} at random, weighted by the probabilities in {@code transitions}.
     *
     * @param transitions map from operation to probability.  All values must be ≥0 and
     *                    at least one value must be >0.
     * @return a randomly chosen key, according to its weight.
     * @throws IllegalArgumentException if the map is empty or contains no positive probability.
     */
    public static UserOpType pickRandomOp(Map<UserOpType, Double> transitions) {
        if (transitions == null || transitions.isEmpty()) {
            throw new IllegalArgumentException("Transition map is empty.");
        }

        // 1. Compute the total weight (in case it isn’t exactly 1.0)
        double total = 0.0;
        for (double p : transitions.values()) {
            if (p < 0) {
                throw new IllegalArgumentException("Negative probability: " + p);
            }
            total += p;
        }
        if (total == 0.0) {
            throw new IllegalArgumentException("All probabilities are zero.");
        }

        // 2. Draw a number uniformly in [0, total)
        double r = RNG.nextDouble() * total;

        // 3. Walk through the cumulative distribution
        double cumulative = 0.0;
        for (Map.Entry<UserOpType, Double> e : transitions.entrySet()) {
            cumulative += e.getValue();
            if (r < cumulative) {
                return e.getKey();
            }
        }

        // We should never get here unless rounding error pushed us off the end.
        // Fall back to returning the last entry.
        return transitions.keySet().stream().reduce((first, second) -> second).orElseThrow();
    }
}
