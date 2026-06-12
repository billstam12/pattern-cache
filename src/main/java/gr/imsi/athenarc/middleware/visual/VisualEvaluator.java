package gr.imsi.athenarc.middleware.visual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import gr.imsi.athenarc.middleware.domain.*;
import gr.imsi.athenarc.middleware.sketch.PixelColumn;

import java.util.*;

public class VisualEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(VisualEvaluator.class);

    private MaxErrorEvaluator maxErrorEvaluator;
    private boolean hasError = true;
    private AggregateInterval pixelColumnInterval;
    private double error;
    public int validColumns = 0;
    private List<PixelColumn> pixelColumns;
    private List<Double> pixelColumnErrors;

    public double calculateTotalError(List<PixelColumn> pixelColumns, ViewPort viewPort, AggregateInterval pixelColumnInterval, double accuracy) {
        // Calculate errors using processed data
        maxErrorEvaluator = new MaxErrorEvaluator(viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        this.pixelColumns = pixelColumns;
        this.pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumn();
        List<Double> pixelColumnErrors = this.pixelColumnErrors;
        LOG.debug("Pixel column errors: {}", pixelColumnErrors);
        // Definition 3.5 (MinMaxCache paper): ε = Σ|E^i| / (w·h). Each entry of
        // pixelColumnErrors is already |E^i|/h, so summing and dividing by w gives ε.
        // Columns that cannot be evaluated (no fully-contained group → null entry) are
        // reported separately via getInconclusiveIntervals() and trigger a fetch; we exclude
        // them from the average rather than treating them as 0 or h pixels of error,
        // which would bias the reported ε one way or the other. With every column
        // evaluable, validColumns == w and the divisor matches the paper exactly.
        validColumns = 0;
        error = 0.0;
        for (Double pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError != null) {
                validColumns += 1;
                error += pixelColumnError;
            }
        }
        error /= validColumns;
        hasError = error > 1 - accuracy;
        return error;
    }

    /**
     * Pixel-column ranges whose error couldn't be evaluated (no fully-contained
     * group). These are <em>not</em> cache misses — they're columns where the
     * evaluator has aggregate data but not enough of it to derive a bound.
     * Fetching the underlying range patches the gap. Distinct from {@code
     * getHighErrorIntervals(slack)}, which returns columns whose error <em>could</em>
     * be evaluated and exceeded the slack.
     */
    public List<TimeInterval> getInconclusiveIntervals() {
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
        LOG.debug("Unable to Determine Errors: " + missingIntervals);
        return missingIntervals;
    }

    /**
     * Pixel-column ranges where the per-column error |E^i|/h exceeds {@code targetSlack}.
     * Mirrors the pattern path's per-match ambiguity test ({@code meanBoundOvershoot >
     * targetSlack}): a column is "ambiguous" when, by itself, it could violate the global
     * SLO ε ≤ targetSlack. Contiguous high-error columns are grouped into single
     * intervals via {@link DateTimeUtil#groupIntervals}.
     *
     * <p>Columns whose error couldn't be evaluated (null entries) are excluded — they
     * are reported separately by {@link #getInconclusiveIntervals()} and already trigger a
     * fetch on the non-error code path.
     */
    public List<TimeInterval> getHighErrorIntervals(double targetSlack) {
        if (pixelColumnErrors == null || pixelColumns == null) {
            return new ArrayList<>();
        }
        List<TimeInterval> highError = new ArrayList<>();
        for (int i = 0; i < pixelColumnErrors.size(); i++) {
            Double err = pixelColumnErrors.get(i);
            if (err != null && err > targetSlack) {
                PixelColumn pc = pixelColumns.get(i);
                highError.add(new TimeRange(pc.getFrom(), pc.getTo()));
            }
        }
        return DateTimeUtil.groupIntervals(pixelColumnInterval, highError);
    }

    public List<RangeSet<Integer>> getMissingPixels() {
        return maxErrorEvaluator.getMissingPixels();
    }

    public List<RangeSet<Integer>> getFalsePixels() {
        return maxErrorEvaluator.getFalsePixels();
    }

    public boolean hasError(){
        return hasError;
    }

    public Stats getViewPortStats() {
        return maxErrorEvaluator.getViewPortStatsAggregator();
    }
    
    /**
     * Class that computes the maximum number of pixel errors.
     */
    private static class MaxErrorEvaluator {
        private static final Logger LOG = LoggerFactory.getLogger(MaxErrorEvaluator.class);

        private final ViewPort viewPort;

        private final List<PixelColumn> pixelColumns;
        private StatsAggregator viewPortStatsAggregator;
        private List<TimeInterval> missingRanges;

        private List<RangeSet<Integer>> missingPixels;
        private List<RangeSet<Integer>> falsePixels;

        protected MaxErrorEvaluator(ViewPort viewPort, List<PixelColumn> pixelColumns) {
            this.viewPort = viewPort;
            this.pixelColumns = pixelColumns;
            this.missingPixels = new ArrayList<>();
            this.falsePixels = new ArrayList<>();
        }

        protected List<Double> computeMaxPixelErrorsPerColumn() {
            List<Double> maxPixelErrorsPerColumn = new ArrayList<>();
            missingRanges = new ArrayList<>();

            // The stats aggregator for the whole query interval to keep track of the min/max values
            // and determine the y-axis scale.
            viewPortStatsAggregator = new StatsAggregator();
            pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));
            LOG.debug("Viewport stats: {}", viewPortStatsAggregator);

            for (int i = 0; i < pixelColumns.size(); i++) {
                PixelColumn currentPixelColumn = pixelColumns.get(i);
                RangeSet<Integer> pixelColumnFalsePixels = TreeRangeSet.create();
                RangeSet<Integer> pixelColumnMissingPixels = TreeRangeSet.create();

                // There are no data points or we have raw data points
                // Initialized means a time series span (created from the database) tried to add points to this column
                // If after that operation the column's count is still 0, it means that there are no data points in the range of the column
                if((currentPixelColumn.getStats().getCount() == 0 && currentPixelColumn.hasInitialized()) || currentPixelColumn.hasNoError()){
                    maxPixelErrorsPerColumn.add(0.0);
                    missingPixels.add(pixelColumnMissingPixels);
                    falsePixels.add(pixelColumnFalsePixels);
                    continue;
                }

                PixelColumn previousPixelColumn = null, nextPixelColumn = null;
                Range<Integer> leftMaxFalsePixels = null, rightMaxFalsePixels = null;
                if (!currentPixelColumn.canEvaluateInnerColumn()) {
                    maxPixelErrorsPerColumn.add(null);
                    missingPixels.add(pixelColumnMissingPixels);
                    falsePixels.add(pixelColumnFalsePixels);
                    missingRanges.add(currentPixelColumn.getRange()); // add range as missing to fetch
                    continue;
                }
                else {
                    // Check if there is a previous PixelColumn
                    if (i > 0) {
                        previousPixelColumn = pixelColumns.get(i - 1);
                        if (!previousPixelColumn.hasNoError() && previousPixelColumn.getStats().getCount() != 0 ) {
                            leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(previousPixelColumn.getStats().getLastTimestamp(), previousPixelColumn.getStats().getLastValue(), currentPixelColumn.getStats().getFirstTimestamp(), currentPixelColumn.getStats().getFirstValue(), viewPort, viewPortStatsAggregator);

                            if (!getMaxMissingInterColumnPixels(previousPixelColumn, currentPixelColumn, pixelColumnMissingPixels, viewPortStatsAggregator)){
                                maxPixelErrorsPerColumn.add(null);
                                missingPixels.add(pixelColumnMissingPixels);
                                falsePixels.add(pixelColumnFalsePixels);
                                continue;
                            }
                            pixelColumnFalsePixels.add(leftMaxFalsePixels);
                        }
                    }
                    // Check if there is a next PixelColumn
                    if (i < pixelColumns.size() - 1) {
                        nextPixelColumn = pixelColumns.get(i + 1);
                        if (!nextPixelColumn.hasNoError() && nextPixelColumn.getStats().getCount() != 0  ) {
                            rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(currentPixelColumn.getStats().getLastTimestamp(), currentPixelColumn.getStats().getLastValue(), nextPixelColumn.getStats().getFirstTimestamp(), nextPixelColumn.getStats().getFirstValue(), viewPort, viewPortStatsAggregator);
                            
                            if (!getMaxMissingInterColumnPixels(currentPixelColumn, nextPixelColumn, pixelColumnMissingPixels, viewPortStatsAggregator)){
                                maxPixelErrorsPerColumn.add(null);
                                missingPixels.add(pixelColumnMissingPixels);
                                falsePixels.add(pixelColumnFalsePixels);
                                continue;
                            }
                            pixelColumnFalsePixels.add(rightMaxFalsePixels);
                        }
                    }

                    // Subtract P_i (paper Theorem 3.3) from F_{i,j} to get the false-pixel
                    // contribution to E^i.
                    Range<Integer> actualInnerColumnPixelRange = currentPixelColumn.getActualInnerColumnPixelRange(viewPort, viewPortStatsAggregator);
                    pixelColumnFalsePixels.remove(actualInnerColumnPixelRange);

                    // A pixel is "missing" only if no line — correct or incorrect — passes
                    // through it. Subtract the union of inner-column foreground pixels (P_i)
                    // and the rasterised inter-column line pixels (F_{i,j}) from the missing
                    // set. Earlier this used a single bounding-box span(), which over-removed
                    // through the gaps between the three ranges and under-counted errors.
                    RangeSet<Integer> drawnPixels = TreeRangeSet.create();
                    if (leftMaxFalsePixels != null) drawnPixels.add(leftMaxFalsePixels);
                    if (rightMaxFalsePixels != null) drawnPixels.add(rightMaxFalsePixels);
                    drawnPixels.add(actualInnerColumnPixelRange);
                    pixelColumnMissingPixels.removeAll(drawnPixels);

                    RangeSet<Integer> pixelColumnErrorPixels = TreeRangeSet.create();
                    pixelColumnErrorPixels.addAll(pixelColumnFalsePixels);
                    pixelColumnErrorPixels.addAll(pixelColumnMissingPixels);
                    // Count pixels via canonical form: integer Ranges normalised to
                    // [low, high) so the count is high - low regardless of whether the
                    // original Range was closed/open/half-open. Earlier this used
                    // `upper - lower + 1` and overcounted ranges produced by RangeSet
                    // operations (which routinely have half-open endpoints).
                    int maxWrongPixels = pixelColumnErrorPixels.asRanges().stream()
                            .mapToInt(range -> {
                                Range<Integer> canonical = range.canonical(DiscreteDomain.integers());
                                return canonical.upperEndpoint() - canonical.lowerEndpoint();
                            })
                            .sum();
                    if(Math.abs(maxWrongPixels) > viewPort.getHeight()) {
                        LOG.warn("Max wrong pixels {} exceeds viewPort height {}", maxWrongPixels, viewPort.getHeight());
                        continue;
                    }   
                    // Normalize the result
                    maxPixelErrorsPerColumn.add(((double) maxWrongPixels / viewPort.getHeight()));

                    // Add sets to list for return on queryResults
                    missingPixels.add(pixelColumnMissingPixels);
                    falsePixels.add(pixelColumnFalsePixels);
                }
            }
            return maxPixelErrorsPerColumn;
        }

        /**
         * Computes the M_{i,j} inter-column missing-pixel range from Theorem 3.4 of the
         * MinMaxCache paper for the boundary between {@code leftPixelColumn} and
         * {@code rightPixelColumn}, and adds it to {@code pixelColumnMissingPixels}.
         *
         * <p>Case (i) — a single partial group straddles the boundary:
         * {@code M_{i,j} = [p_y(B^min), p_y(B^max)]}.
         *
         * <p>Case (ii) — no partial; uses the rightmost fully-contained group of the left
         * column and the leftmost of the right column to bound the missing line.
         *
         * @return {@code false} when neither a boundary partial nor flanking fully-contained
         *         groups exist — error cannot be evaluated for this boundary.
         */
        boolean getMaxMissingInterColumnPixels(PixelColumn leftPixelColumn, PixelColumn rightPixelColumn,
                                               RangeSet<Integer> pixelColumnMissingPixels,
                                               StatsAggregator viewPortStatsAggregator) {
            // The partial at the leftPixelColumn/rightPixelColumn boundary is, by construction,
            // rightPixelColumn.getLeftPartial() (equivalently leftPixelColumn.getRightPartial());
            // both name the same physical group. Earlier this read leftPixelColumn.getLeftPartial(),
            // which referred to the partial at the *previous* boundary — wrong group, wrong M_{i,j}.
            AggregatedDataPoint boundaryPartial = rightPixelColumn.getLeftPartial();
            if (boundaryPartial == null) {
                if (rightPixelColumn.getLeft().size() > 0 && leftPixelColumn.getRight().size() > 0) {
                    AggregatedDataPoint left = rightPixelColumn.getLeft().get(0);
                    AggregatedDataPoint right = leftPixelColumn.getRight().get(0);
                    int leftMinPixelId = viewPort.getPixelId(left.getStats().getMinValue(), viewPortStatsAggregator);
                    int rightMaxPixelId = viewPort.getPixelId(right.getStats().getMaxValue(), viewPortStatsAggregator);
                    int leftMaxPixelId = viewPort.getPixelId(left.getStats().getMaxValue(), viewPortStatsAggregator);
                    int rightMinPixelId = viewPort.getPixelId(right.getStats().getMinValue(), viewPortStatsAggregator);

                    pixelColumnMissingPixels.add(Range.closed(Math.min(leftMinPixelId, rightMaxPixelId), Math.max(leftMinPixelId, rightMaxPixelId)));
                    pixelColumnMissingPixels.add(Range.closed(Math.min(leftMaxPixelId, rightMinPixelId), Math.max(leftMaxPixelId, rightMinPixelId)));
                } else {
                    return false;
                }
            } else {
                pixelColumnMissingPixels.add(Range.closed(
                        viewPort.getPixelId(boundaryPartial.getStats().getMinValue(), viewPortStatsAggregator),
                        viewPort.getPixelId(boundaryPartial.getStats().getMaxValue(), viewPortStatsAggregator)));
            }
            return true;
        }

        public List<TimeInterval> getMissingRanges() {
            return missingRanges;
        }

        public List<RangeSet<Integer>> getMissingPixels() {
            return missingPixels;
        }

        public List<RangeSet<Integer>> getFalsePixels() {
            return falsePixels;
        }

        public Stats getViewPortStatsAggregator() {
            return viewPortStatsAggregator;
        }
    }
}
