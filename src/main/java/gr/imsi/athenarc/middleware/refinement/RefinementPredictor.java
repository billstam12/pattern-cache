package gr.imsi.athenarc.middleware.refinement;

import java.util.OptionalInt;

/**
 * Adaptive sub-interval refinement: pick the next aggregation factor when the
 * observed bound slack exceeds the query's accuracy threshold. Single doubling
 * step per call; refuses to clamp — if a full doubling would breach the cap,
 * returns an empty {@link OptionalInt} so the caller can fall back instead of
 * settling for a half-doubling.
 */
public final class RefinementPredictor {

    /**
     * Hard ceiling on aggFactor, well below int overflow. At α=2^20 a 1-minute
     * timeUnit would imply 60μs sub-buckets — already past any meaningful
     * resolution; α this high indicates a noise-floor situation refinement
     * cannot fix.
     */
    public static final int MAX_AGG_FACTOR = 1 << 20;

    private RefinementPredictor() {}

    /**
     * Doubling step capped at {@link #MAX_AGG_FACTOR}.
     *
     * @return present with {@code 2 × currentAggFactor} when refinement is
     *         needed; present with {@code currentAggFactor} when observed
     *         already meets target (defensive — caller usually only invokes
     *         this when refinement is needed); empty when the doubled α would
     *         exceed the effective cap.
     */
    public static OptionalInt nextAggFactor(int currentAggFactor,
                                            double observedSlack,
                                            double targetSlack) {
        return nextAggFactor(currentAggFactor, observedSlack, targetSlack, MAX_AGG_FACTOR);
    }

    /**
     * As {@link #nextAggFactor(int, double, double)} but with an explicit cap.
     * {@code dataResolutionCap} lets the caller express the dataset's raw-
     * resolution ceiling (i.e. {@code timeUnit / samplingInterval}); past that
     * point the SQL aggregation buckets are sparse, the LP bound stops
     * improving, and the NFA can lose matches to empty sub-buckets.
     */
    public static OptionalInt nextAggFactor(int currentAggFactor,
                                            double observedSlack,
                                            double targetSlack,
                                            int dataResolutionCap) {
        if (currentAggFactor < 1) {
            throw new IllegalArgumentException("currentAggFactor must be >= 1");
        }
        int effectiveCap = dataResolutionCap > 0
                ? Math.min(MAX_AGG_FACTOR, dataResolutionCap)
                : MAX_AGG_FACTOR;
        if (observedSlack <= targetSlack) {
            return OptionalInt.of(Math.min(currentAggFactor, effectiveCap));
        }
        int doubled = currentAggFactor * 2;
        if (doubled > effectiveCap) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(doubled);
    }
}
