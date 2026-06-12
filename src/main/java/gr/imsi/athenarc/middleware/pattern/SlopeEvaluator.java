package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.sketch.Sketch;

/**
 * Slope-field error evaluator — the pattern-side mirror of visual's {@code
 * VisualEvaluator}. It is query-agnostic: it never runs the NFA. Instead 
 * it measures how faithfully the current aggregates represent the underlying
 *  slope at each time unit, exactly the way the visual path measures how 
 * faithfully min/max represent each pixel column.
 *
 * <p>Per time unit (one {@link Sketch}) the error is the normalized angle-bound
 * margin {@code (maxAngle - minAngle) / 180} = {@link Sketch#getAngleErrorMargin()},
 * in [0, 1]. The global error is the mean over <em>evaluable</em> units:
 * <pre>  ē = Σ marginᵢ / (evaluable units)</pre>
 * and {@code hasError = ē &gt; 1 - accuracy}, the same shape as the visual
 * Definition-3.5 average. A unit is high-error when its own margin exceeds the
 * slack — by itself it could violate the global SLO.
 *
 * <p><b>Evaluability gate (soundness condition).</b> A unit contributes to ē
 * only when its bound is sound: the sketch is initialised and its sub-buckets
 * fully tile the unit so {@link Sketch#getAngleErrorMargin()} is finite. This is
 * the slope analogue of the visual evaluator requiring at least one
 * fully-contained, gap-free aggregation group per column. Two non-evaluable
 * cases, handled separately like the visual path:
 * <ul>
 *   <li><b>uninitialised</b> ({@code !hasInitialized()}) — data-missing; the
 *       executor patches it at the current α via {@code
 *       PatternDataProcessor.identifyDataMissingIntervals}. Reported here as a
 *       {@code null} per-unit error and excluded from ē.</li>
 *   <li><b>initialised but unresolved</b> (non-finite margin — fewer than two
 *       sub-buckets, no slope definable yet) — counted as max error (1.0) so it
 *       stays above any slack and is surfaced by {@link
 *       #getHighErrorIntervals(double)} for refinement at a finer α.</li>
 * </ul>
 */
public class SlopeEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(SlopeEvaluator.class);

    private List<Sketch> sketches;
    private AggregateInterval timeUnit;
    private Double[] perUnitError;
    private double error;
    private boolean hasError = true;
    private int validUnits = 0;

    /**
     * Compute the mean per-unit slope-bound margin over the given sketches and
     * set {@link #hasError()} against {@code accuracy}. Stateful — subsequent
     * calls overwrite the stored per-unit errors.
     */
    public double calculateTotalError(List<Sketch> sketches, AggregateInterval timeUnit, double accuracy) {
        this.sketches = sketches;
        this.timeUnit = timeUnit;
        this.perUnitError = new Double[sketches.size()];

        double sum = 0.0;
        validUnits = 0;
        for (int i = 0; i < sketches.size(); i++) {
            Sketch sketch = sketches.get(i);
            if (!sketch.hasInitialized()) {
                perUnitError[i] = null; // data-missing → patched separately, excluded from ē
                continue;
            }
            double margin = sketch.getAngleErrorMargin();
            // Initialised but no sound slope bound (too few sub-buckets): treat as
            // max error so it stays high and is picked up for finer-α refinement.
            double unitError = Double.isFinite(margin) ? margin : 1.0;
            perUnitError[i] = unitError;
            sum += unitError;
            validUnits++;
        }
        error = validUnits == 0 ? 0.0 : sum / validUnits;
        hasError = error > 1 - accuracy;
        LOG.info("Slope-field error: {} over {} evaluable units (hasError={})", error, validUnits, hasError);
        return error;
    }

    public boolean hasError() {
        return hasError;
    }

    public double getError() {
        return error;
    }

    public int getValidUnits() {
        return validUnits;
    }

    /**
     * Time-unit ranges whose own slope margin exceeds {@code targetSlack} — the
     * units the refinement ladder targets for a finer-α refetch. Mirror of
     * visual's {@code getHighErrorIntervals}. Uninitialised units (null error)
     * are excluded; they are handled by the data-missing patch. Contiguous
     * high-error units are merged on the {@code timeUnit} grid.
     */
    public List<TimeInterval> getHighErrorIntervals(double targetSlack) {
        List<TimeInterval> highError = new ArrayList<>();
        if (perUnitError == null || sketches == null) return highError;
        for (int i = 0; i < perUnitError.length; i++) {
            Double err = perUnitError[i];
            if (err != null && err > targetSlack) {
                Sketch sketch = sketches.get(i);
                highError.add(new TimeRange(sketch.getFrom(), sketch.getTo()));
            }
        }
        return DateTimeUtil.groupIntervals(timeUnit, highError);
    }
}
