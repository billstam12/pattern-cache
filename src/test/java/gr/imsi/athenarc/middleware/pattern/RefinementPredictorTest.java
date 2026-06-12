package gr.imsi.athenarc.middleware.pattern;

import java.util.OptionalInt;

import org.junit.Test;

import gr.imsi.athenarc.middleware.refinement.RefinementPredictor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RefinementPredictorTest {

    @Test
    public void doublingAlwaysReturnsTwoX() {
        assertEquals(OptionalInt.of(8),
                RefinementPredictor.nextAggFactor(4, /*observed*/ 0.10, /*target*/ 0.05));
        assertEquals(OptionalInt.of(8),
                RefinementPredictor.nextAggFactor(4, /*observed*/ 1.00, /*target*/ 0.01));
    }

    @Test
    public void noRefinementWhenObservedAlreadyBelowTarget() {
        // Defensive: caller normally only invokes this when observed > target,
        // but if it doesn't, we return the same α (no-op refinement).
        assertEquals(OptionalInt.of(4),
                RefinementPredictor.nextAggFactor(4, 0.04, 0.05));
    }

    @Test
    public void cappedDoublingReturnsEmpty() {
        // Doubling 4 → 8 would exceed cap=7. Refuse to clamp — return empty.
        assertFalse(RefinementPredictor.nextAggFactor(4, 0.10, 0.05, /*cap*/ 7).isPresent());
    }

    @Test
    public void alreadyAtCapReturnsEmptyOnRefineCall() {
        assertFalse(RefinementPredictor.nextAggFactor(1440, 0.30, 0.05, /*cap*/ 1440).isPresent());
    }

    @Test
    public void dataResolutionCapZeroOrNegativeIgnored() {
        // Defensive: bad cap input should not break the predictor — fall back to
        // the regular MAX_AGG_FACTOR ceiling.
        assertEquals("cap=0 must not zero-out refinement", OptionalInt.of(8),
                RefinementPredictor.nextAggFactor(4, 0.10, 0.05, /*cap*/ 0));
        assertEquals("cap<0 must not zero-out refinement", OptionalInt.of(8),
                RefinementPredictor.nextAggFactor(4, 0.10, 0.05, /*cap*/ -7));
    }
}
