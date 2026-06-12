package gr.imsi.athenarc.middleware.visual;

import com.google.common.collect.RangeSet;
import org.junit.Test;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the visual error calculation in {@link VisualEvaluator} matches the
 * MinMaxCache paper's Theorems 3.3, 3.4, and Definition 3.5 (Maroulis et al., VLDB 2024).
 *
 * <p>All tests construct PixelColumns through their public {@code addAggregatedDataPoint}
 * API with hand-chosen group boundaries and stats so the expected per-pixel error set
 * can be derived by hand from the paper formulas.
 */
public class VisualEvaluatorTest {

    /** Build an aggregated group with explicit min/max value+timestamp. */
    private static AggregatedDataPoint group(long from, long to,
                                             long minTs, double minVal,
                                             long maxTs, double maxVal) {
        StatsAggregator stats = new StatsAggregator();
        if (minTs <= maxTs) {
            stats.accept(new ImmutableDataPoint(minTs, minVal, 0));
            stats.accept(new ImmutableDataPoint(maxTs, maxVal, 0));
        } else {
            stats.accept(new ImmutableDataPoint(maxTs, maxVal, 0));
            stats.accept(new ImmutableDataPoint(minTs, minVal, 0));
        }
        return new ImmutableAggregatedDataPoint(from, to, 0, stats);
    }

    /**
     * Regression for the off-by-one partial-access bug in {@code getMaxMissingInterColumnPixels}.
     *
     * <p>Three pixel columns. Boundary p0/p1 has a single-pixel-wide partial; boundary
     * p1/p2 has a partial spanning the whole y-range. Pre-fix, the boundary check at
     * p1/p2 would access {@code p1.getLeftPartial()} (the partial at the previous
     * boundary, p0/p1) and report its narrow range; post-fix it accesses
     * {@code p2.getLeftPartial()} and correctly reports the full range.
     *
     * <p>Layout:
     * <pre>
     *  p0 = [0,100)   p1 = [100,200)   p2 = [200,300)         w=3, h=10
     *  G0_full      [0,80)    min=5@40, max=6@50   ⊂ p0
     *  G01_partial  [80,120)  min=5@90, max=5@95   straddles p0/p1   (narrow)
     *  G1_full      [120,180) min=3@130, max=5@170 ⊂ p1
     *  G12_partial  [180,220) min=0@210, max=8@195 straddles p1/p2   (full range)
     *  G2_full      [220,300) min=5@230, max=6@270 ⊂ p2
     * </pre>
     * Viewport y range over all stats: [0, 8]. p_y(v) = floor(9·v/8):
     * 0→0, 3→3, 5→5, 6→6, 8→9.
     *
     * <p>Expected per Theorem 3.4 case (i):
     * <ul>
     *   <li>M_{1,2} = G12_partial pixel range = [p_y(0), p_y(8)] = [0,9]</li>
     *   <li>M_{0,1} = G01_partial pixel range = [p_y(5), p_y(5)] = [5,5]</li>
     * </ul>
     * After subtracting "drawn" pixels (P_1 ∪ F_{0,1} ∪ F_{1,2}) from missing,
     * post-fix p1.missingPixels contains pixels 0..2; pre-fix it would be empty
     * (because the wrong partial collapses M_{1,2} to {5}, which is fully covered
     * by P_1=[3,5]).
     */
    @Test
    public void boundaryPartialAccess_isOffByOnePreFix() {
        AggregatedDataPoint G0_full     = group(  0,  80,  40, 5.0,  50, 6.0);
        AggregatedDataPoint G01_partial = group( 80, 120,  90, 5.0,  95, 5.0);
        AggregatedDataPoint G1_full     = group(120, 180, 130, 3.0, 170, 5.0);
        AggregatedDataPoint G12_partial = group(180, 220, 210, 0.0, 195, 8.0);
        AggregatedDataPoint G2_full     = group(220, 300, 230, 5.0, 270, 6.0);

        PixelColumn p0 = new PixelColumn(  0, 100);
        PixelColumn p1 = new PixelColumn(100, 200);
        PixelColumn p2 = new PixelColumn(200, 300);

        // Each column receives only the groups overlapping it (matches realistic usage).
        p0.addAggregatedDataPoint(G0_full);
        p0.addAggregatedDataPoint(G01_partial);

        p1.addAggregatedDataPoint(G01_partial);
        p1.addAggregatedDataPoint(G1_full);
        p1.addAggregatedDataPoint(G12_partial);

        p2.addAggregatedDataPoint(G12_partial);
        p2.addAggregatedDataPoint(G2_full);

        ViewPort viewPort = new ViewPort(3, 10);
        AggregateInterval pixelInterval = AggregateInterval.of(100, ChronoUnit.MILLIS);

        VisualEvaluator evaluator = new VisualEvaluator();
        evaluator.calculateTotalError(Arrays.asList(p0, p1, p2), viewPort, pixelInterval, /*accuracy*/ 0.95);

        List<RangeSet<Integer>> missingPixels = evaluator.getMissingPixels();
        // Post-fix: M_{1,2} from G12_partial covers pixels [0, 9]. After subtracting
        // P_1=[3,5] and the inter-column F lines [3,5] ∪ [5,9] = [3,9], pixel 0 must
        // remain in p1's missing set.
        assertTrue("p1.missingPixels should contain pixel 0 (from G12_partial.minValue=0). "
                + "Pre-fix used the wrong partial (G01_partial) and this pixel was lost.",
                missingPixels.get(1).contains(0));
        assertTrue("p1.missingPixels should contain pixel 1",  missingPixels.get(1).contains(1));
        assertTrue("p1.missingPixels should contain pixel 2",  missingPixels.get(1).contains(2));
        // Pixels 3..9 are removed by drawnPixels (P_1 ∪ F lines).
        assertFalse("pixel 5 is inside P_1 = [3,5]; cannot be in missingPixels",
                missingPixels.get(1).contains(5));
    }

    /**
     * Definition 3.5: ε = Σ|E^i| / (w·h). Reuses the layout above and verifies the
     * total error matches the hand-computed value.
     *
     * <p>Hand calculation per Theorems 3.3/3.4 (post-fix code, paper-faithful):
     * <ul>
     *   <li>p0: F_{0,1}=[5,5], M_{0,1}=[5,5], P_0=[5,6]. (F ∪ M) \ P_0 = ∅. |E^0|=0</li>
     *   <li>p1: F=[3,5]∪[5,9]=[3,9], M=[5,5]∪[0,9]=[0,9], P_1=[3,5].
     *           E^1 = ([3,9] ∪ [0,9]) \ [3,5] = [0,2] ∪ [6,9]. |E^1|=7</li>
     *   <li>p2: F_{1,2}=[0,5], M_{1,2}=[0,9], P_2=[5,6]. (F ∪ M) \ P_2 = [0,4] ∪ [7,9]. |E^2|=8</li>
     * </ul>
     * ε = (0 + 7 + 8) / (3·10) = 15/30 = 0.5.
     */
    @Test
    public void totalErrorMatchesPaperDefinition35() {
        AggregatedDataPoint G0_full     = group(  0,  80,  40, 5.0,  50, 6.0);
        AggregatedDataPoint G01_partial = group( 80, 120,  90, 5.0,  95, 5.0);
        AggregatedDataPoint G1_full     = group(120, 180, 130, 3.0, 170, 5.0);
        AggregatedDataPoint G12_partial = group(180, 220, 210, 0.0, 195, 8.0);
        AggregatedDataPoint G2_full     = group(220, 300, 230, 5.0, 270, 6.0);

        PixelColumn p0 = new PixelColumn(  0, 100);
        PixelColumn p1 = new PixelColumn(100, 200);
        PixelColumn p2 = new PixelColumn(200, 300);

        p0.addAggregatedDataPoint(G0_full);
        p0.addAggregatedDataPoint(G01_partial);
        p1.addAggregatedDataPoint(G01_partial);
        p1.addAggregatedDataPoint(G1_full);
        p1.addAggregatedDataPoint(G12_partial);
        p2.addAggregatedDataPoint(G12_partial);
        p2.addAggregatedDataPoint(G2_full);

        ViewPort viewPort = new ViewPort(3, 10);
        AggregateInterval pixelInterval = AggregateInterval.of(100, ChronoUnit.MILLIS);

        VisualEvaluator evaluator = new VisualEvaluator();
        double error = evaluator.calculateTotalError(
                Arrays.asList(p0, p1, p2), viewPort, pixelInterval, /*accuracy*/ 0.95);

        // ε = 15 / 30 = 0.5
        assertEquals(0.5, error, 1e-9);
    }

    /**
     * Theorem 3.4 case (ii) — no partial at the boundary. Verifies the inter-column
     * missing range derives from the rightmost fully-contained group of the left
     * column and the leftmost of the right column.
     *
     * <p>Layout (no partials anywhere):
     * <pre>
     *  p0 = [0,100), p1 = [100,200).   w=2, h=10
     *  G0a  [0,50)   min=2@20, max=4@30  ⊂ p0
     *  G0b  [50,100) min=1@70, max=3@90  ⊂ p0  (rightmost in p0)
     *  G1a  [100,150) min=5@110, max=7@140 ⊂ p1  (leftmost in p1)
     *  G1b  [150,200) min=3@170, max=6@180 ⊂ p1
     * </pre>
     * Viewport y range: [1, 7]. p_y(v) = floor(9·(v−1)/6):
     *   1→0, 2→1, 3→3, 4→4, 5→6, 6→7, 7→9.
     *
     * <p>Boundary p0/p1: no partial → case (ii).
     *   B_left  = G0b (last fully-contained of p0). min=1, max=3 → [0,3].
     *   B_right = G1a (first fully-contained of p1). min=5, max=7 → [6,9].
     *   M_{0,1} = [min(p_y(B_l.min), p_y(B_r.max)), max(...)] ∪ [...,...]
     *           = closed(0, 9) ∪ closed(3, 6) per code.
     *
     * <p>For p1: P_1 = G1a ∪ G1b min/max. G1a=[6,9], G1b=[3,7]. fullyContained agg
     * tracks combined min=3, max=7 → P_1 = [p_y(3), p_y(7)] = [3, 9].
     * Missing should contain pixel 0 (from M's [0,9] range) which is below P_1.
     */
    @Test
    public void noPartialAtBoundary_caseIIUsesAdjacentFullyContainedGroups() {
        AggregatedDataPoint G0a = group(  0,  50,  20, 2.0,  30, 4.0);
        AggregatedDataPoint G0b = group( 50, 100,  70, 1.0,  90, 3.0);
        AggregatedDataPoint G1a = group(100, 150, 110, 5.0, 140, 7.0);
        AggregatedDataPoint G1b = group(150, 200, 170, 3.0, 180, 6.0);

        PixelColumn p0 = new PixelColumn(  0, 100);
        PixelColumn p1 = new PixelColumn(100, 200);

        p0.addAggregatedDataPoint(G0a);
        p0.addAggregatedDataPoint(G0b);
        p1.addAggregatedDataPoint(G1a);
        p1.addAggregatedDataPoint(G1b);

        ViewPort viewPort = new ViewPort(2, 10);
        AggregateInterval pixelInterval = AggregateInterval.of(100, ChronoUnit.MILLIS);

        VisualEvaluator evaluator = new VisualEvaluator();
        evaluator.calculateTotalError(Arrays.asList(p0, p1), viewPort, pixelInterval, 0.95);

        List<RangeSet<Integer>> missingPixels = evaluator.getMissingPixels();
        // Pixel 0 from M_{0,1}=[0,9] survives subtraction of P_1=[3,9] and any F line
        // (F line from p0.last (90,3) to p1.first (110,5) clamps inside p1 to a small
        // range near pixels 3..6 — does not cover pixel 0).
        assertTrue("p1 should report missing pixel 0 from case-(ii) M_{0,1}",
                missingPixels.get(1).contains(0));
    }

    /**
     * canEvaluateInnerColumn predicate: returns false when there are no
     * fully-contained groups, false when there are gaps in fully-contained groups,
     * true on a clean column.
     */
    @Test
    public void canEvaluateInnerColumn_predicateBehavior() {
        // 1) No fully-contained groups → false.
        PixelColumn empty = new PixelColumn(0, 100);
        assertFalse(empty.canEvaluateInnerColumn());

        // 2) Single fully-contained group → true.
        PixelColumn ok = new PixelColumn(0, 100);
        ok.addAggregatedDataPoint(group(10, 90, 30, 1.0, 60, 5.0));
        assertTrue(ok.canEvaluateInnerColumn());

        // 3) Gap between two fully-contained groups → false.
        PixelColumn gap = new PixelColumn(0, 100);
        gap.addAggregatedDataPoint(group( 0, 30, 10, 1.0, 20, 2.0));
        gap.addAggregatedDataPoint(group(60, 90, 70, 3.0, 80, 4.0));
        assertFalse(gap.canEvaluateInnerColumn());

        // 4) Two fully-contained groups touching exactly → true (single contiguous range).
        PixelColumn touching = new PixelColumn(0, 100);
        touching.addAggregatedDataPoint(group( 0, 50, 10, 1.0, 20, 2.0));
        touching.addAggregatedDataPoint(group(50, 90, 60, 3.0, 80, 4.0));
        assertTrue(touching.canEvaluateInnerColumn());
    }
}
