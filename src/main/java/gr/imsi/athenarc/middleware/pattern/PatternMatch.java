package gr.imsi.athenarc.middleware.pattern;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.sketch.SketchUtils;

/**
 * Represents a pattern match found by the sketch search algorithm.
 * Contains multiple segments and provides methods for calculating match statistics
 * and logging match details.
 */
public class PatternMatch implements Serializable {
        
    private final List<MatchSegment> segments;
    private final long startTime;
    private final long endTime;
    private final double averageErrorMargin;
    private final int matchLength;
    
    /**
     * Creates a new PatternMatch from a list of sketch segments without per-segment
     * filter context. Used by callers (no-cache OLS path, MATCH_RECOGNIZE) that
     * never need confidence classification because their sketches have zero error
     * margin.
     */
    public PatternMatch(List<List<Sketch>> sketchSegments) {
        this(sketchSegments, Collections.nCopies(sketchSegments == null ? 0 : sketchSegments.size(), (ValueFilter) null));
    }

    /**
     * Creates a new PatternMatch from sketch segments paired with the ValueFilter
     * each segment was validated against. The filter is required for the cached
     * pattern path's relaxed-NFA + bound-classifier flow: each MatchSegment knows
     * its own filter and can answer {@code isConfident()} on demand.
     *
     * @param sketchSegments segments, each a list of sketches
     * @param segmentFilters the value filter applied per segment, parallel to {@code sketchSegments}
     */
    public PatternMatch(List<List<Sketch>> sketchSegments, List<ValueFilter> segmentFilters) {
        if (sketchSegments == null || sketchSegments.isEmpty()) {
            throw new IllegalArgumentException("Match must contain at least one segment");
        }
        if (segmentFilters == null || segmentFilters.size() != sketchSegments.size()) {
            throw new IllegalArgumentException(
                    "segmentFilters must be non-null and parallel to sketchSegments");
        }

        this.segments = new ArrayList<>();

        for (int i = 0; i < sketchSegments.size(); i++) {
            segments.add(new MatchSegment(sketchSegments.get(i), i, segmentFilters.get(i)));
        }

        this.startTime = calculateStartTime();
        this.endTime = calculateEndTime();
        this.averageErrorMargin = calculateAverageErrorMargin();
        this.matchLength = calculateTotalLength();
    }
    
    private long calculateStartTime() {
        return segments.stream()
            .mapToLong(MatchSegment::getStartTime)
            .min()
            .orElse(Long.MAX_VALUE);
    }
    
    private long calculateEndTime() {
        return segments.stream()
            .mapToLong(MatchSegment::getEndTime)
            .max()
            .orElse(Long.MIN_VALUE);
    }
    
    private double calculateAverageErrorMargin() {
        double totalErrorMargin = segments.stream()
            .mapToDouble(MatchSegment::getErrorMargin)
            .filter(error -> !Double.isNaN(error) && !Double.isInfinite(error))
            .sum();
        
        long validSegments = segments.stream()
            .mapToDouble(MatchSegment::getErrorMargin)
            .filter(error -> !Double.isNaN(error) && !Double.isInfinite(error))
            .count();
        
        return validSegments > 0 ? totalErrorMargin / validSegments : 0.0;
    }
    
    private int calculateTotalLength() {
        return segments.stream()
            .mapToInt(MatchSegment::getSketchCount)
            .sum();
    }
    
    /**
     * Gets the segments that form this match.
     * 
     * @return The list of pattern segments
     */
    public List<MatchSegment> getSegments() {
        return segments;
    }
    
    /**
     * Gets the start time of this match.
     * 
     * @return The start timestamp
     */
    public long getStartTime() {
        return startTime;
    }
    
    /**
     * Gets the end time of this match.
     * 
     * @return The end timestamp
     */
    public long getEndTime() {
        return endTime;
    }
    
    /**
     * Gets the average error margin for this match.
     * 
     * @return The average error margin as a percentage (0.0 to 1.0)
     */
    public double getAverageErrorMargin() {
        return averageErrorMargin;
    }
    
    /**
     * Gets the total length (number of sketches) consumed by this match.
     * 
     * @return The total sketch count
     */
    public int getMatchLength() {
        return matchLength;
    }

    /**
     * Gets the duration of this match.
     * 
     * @return The duration in milliseconds
     */
    public long getDuration() {
        return endTime - startTime;
    }
    
    /**
     * Gets the number of segments in this match.
     * 
     * @return The segment count
     */
    public int getSegmentCount() {
        return segments.size();
    }
    
    
    /**
     * Writes detailed information about this match to a FileWriter.
     * 
     * @param writer The FileWriter to write to
     * @throws IOException If writing fails
     */
    public void writeToFile(FileWriter writer) throws IOException {
        writeToFile(writer, "");
    }

    /**
     * Same as {@link #writeToFile(FileWriter)} but prepends {@code tagPrefix}
     * to the Confident/Ambiguous tag (e.g. "Candidate" → "CandidateConfidentMatch")
     * so the parser can distinguish a snapshot of the approxOls path from the
     * end-to-end returned set.
     */
    public void writeToFile(FileWriter writer, String tagPrefix) throws IOException {
        // Tag with ConfidentMatch / AmbiguousMatch so downstream FP analysis can split
        // bound-confident matches (provably filter-in) from relaxed-NFA-admitted
        // ambiguous ones. OLS-NoC matches (null filters) report as confident.
        String tag = (tagPrefix == null ? "" : tagPrefix) + (isConfident() ? "ConfidentMatch" : "AmbiguousMatch");
        writer.write(String.format("%s : [%d to %d] - Average Error Margin: %.2f%% (%d segments, %d total sketches)\n",
            tag, startTime, endTime, averageErrorMargin * 100.0, segments.size(), matchLength));
        for (MatchSegment segment : segments) {
            writer.write(String.format("  %s\n", segment.toString()));
        }
        writer.write("\n");
    }

    @Override
    public String toString() {
        return String.format("PatternMatch: [%d to %d] - Avg Error: %.2f%% (%d segments, %d sketches)", 
            startTime, endTime, averageErrorMargin * 100.0, segments.size(), matchLength);
    }

    /**
     * Represents a single segment in a pattern match, containing a list of sketches
     * that form a continuous time interval.
     */
    private class MatchSegment implements Serializable {

        private final List<Sketch> sketches;
        private final long startTime;
        private final long endTime;
        private final double errorMargin;
        private final int segmentIndex;
        private final ValueFilter valueFilter;
        /** Lazy-cached combined sketch — isConfident() and verifyAgainstExact may hit this twice
         *  per match in the cached pattern flow (relaxed-NFA bound check + post-NFA classifier),
         *  so caching elides one OLS recombination per call. Transient: serialized matches
         *  re-derive on demand. */
        private transient Sketch combinedSketch;

        public MatchSegment(List<Sketch> sketches, int segmentIndex, ValueFilter valueFilter) {
            if (sketches == null || sketches.isEmpty()) {
                throw new IllegalArgumentException("Segment must contain at least one sketch");
            }

            this.sketches = sketches;
            this.segmentIndex = segmentIndex;
            this.valueFilter = valueFilter;

            this.startTime = sketches.get(0).getFrom();
            this.endTime = sketches.get(sketches.size() - 1).getTo();

            // Mean of per-sketch angleErrorMargin over the atomic sketches that
            // make up this segment. Same granularity at which the NFA decides
            // filter membership, so it lines up with the partial-slack signal
            // computed pre-fetch.
            double sum = 0.0;
            int n = 0;
            for (Sketch s : sketches) {
                double e = s.getAngleErrorMargin();
                if (!Double.isNaN(e) && !Double.isInfinite(e)) {
                    sum += e;
                    n++;
                }
            }
            this.errorMargin = n > 0 ? sum / n : 0.0;
        }
        
        /**
         * Gets the sketches that form this segment.
         * 
         * @return The list of sketches
         */
        public List<Sketch> getSketches() {
            return sketches;
        }
        
        /**
         * Gets the start time of this segment.
         * 
         * @return The start timestamp
         */
        public long getStartTime() {
            return startTime;
        }
        
        /**
         * Gets the end time of this segment.
         * 
         * @return The end timestamp
         */
        public long getEndTime() {
            return endTime;
        }
        
        /**
         * Gets the error margin for this segment.
         * 
         * @return The error margin as a percentage (0.0 to 1.0)
         */
        public double getErrorMargin() {
            return errorMargin;
        }
        
        /**
         * Gets the index of this segment within its parent match.
         * 
         * @return The segment index
         */
        public int getSegmentIndex() {
            return segmentIndex;
        }
        
        /**
         * Gets the length (duration) of this segment.
         * 
         * @return The duration in milliseconds
         */
        public long getDuration() {
            return endTime - startTime;
        }
        
        /**
         * Gets the number of sketches in this segment.
         * 
         * @return The sketch count
         */
        public int getSketchCount() {
            return sketches.size();
        }
        
        /**
         * Creates a combined sketch representing this entire segment.
         *
         * @return The combined sketch
         */
        public Sketch getCombinedSketch() {
            Sketch cached = combinedSketch;
            if (cached == null) {
                cached = SketchUtils.combineSketches(sketches);
                combinedSketch = cached;
            }
            return cached;
        }

        /**
         * Filter the NFA validated this segment against. May be {@code null} when the
         * match was constructed without filter context (no-cache OLS, MATCH_RECOGNIZE).
         */
        public ValueFilter getValueFilter() {
            return valueFilter;
        }

        /**
         * True iff the combined-sketch slope bound is fully contained in this segment's
         * filter — i.e., the match decision cannot flip with finer data. When the filter
         * is {@code null} the segment is treated as confident (zero-uncertainty path).
         */
        public boolean isConfident() {
            if (valueFilter == null || valueFilter.isValueAny()) {
                return true;
            }
            return getCombinedSketch().boundContainedIn(valueFilter);
        }

        @Override
        public String toString() {
            return String.format("MatchSegment[%d]: [%d to %d] - Error Margin: %.2f%% (%d sketches)",
                segmentIndex, startTime, endTime, errorMargin * 100.0, sketches.size());
        }
    }

    /**
     * True iff every segment is confident (combined-sketch bound contained in its
     * filter) — the strict, accuracy-independent notion, equivalent to
     * {@code isConfident(0.0)}.
     */
    public boolean isConfident() {
        for (MatchSegment seg : segments) {
            if (!seg.isConfident()) {
                return false;
            }
        }
        return true;
    }

    /**
     * True iff the match is confident within tolerance {@code tol}: the mean
     * per-segment bound overshoot is at most {@code tol}. Called with
     * {@code tol = 1 - accuracy} by the cached pattern path's classifier to gate
     * which matches are returned; matches above tolerance are ambiguous and drive
     * localized refinement.
     */
    public boolean isConfident(double tol) {
        return getBoundOvershot() <= tol;
    }

    /**
     * Mean fraction of each segment's combined-bound that lies outside its filter,
     * averaged over segments with a non-Any filter. 0 = bound fully contained
     * (would classify confident); 1 = bound entirely outside. Diagnostic metric for
     * "how ambiguous" an ambiguous match is — call only on the ambiguous set.
     * Segments with zero bound width or no filter are skipped.
     */
    // public double getMeanBoundOvershoot() {
    //     double sum = 0.0;
    //     int n = 0;
    //     for (MatchSegment seg : segments) {
    //         ValueFilter f = seg.getValueFilter();
    //         if (f == null || f.isValueAny()) {
    //             continue;
    //         }
    //         Sketch combined = seg.getCombinedSketch();
    //         double width = combined.getMaxAngle() - combined.getMinAngle();
    //         if (!(width > 0) || Double.isNaN(width) || Double.isInfinite(width)) {
    //             continue;
    //         }
    //         sum += combined.boundOvershoot(f);
    //         n++;
    //     }
    //     return n > 0 ? sum / n : 0.0;
    // }

    /**
     * Mean fraction of each segment's combined-bound that lies outside its filter,
     * averaged over segments with a non-Any filter. 0 = bound fully contained
     * (would classify confident); 1 = bound entirely outside. Diagnostic metric for
     * "how ambiguous" an ambiguous match is — call only on the ambiguous set.
     * Segments with zero bound width or no filter are skipped.
     */
    public double getBoundFalseProbability() {
        double survival = 1.0;
        int n = 0;
        for (MatchSegment seg : segments) {
            ValueFilter f = seg.getValueFilter();
            if (f == null || f.isValueAny()) {
                continue;
            }
            Sketch combined = seg.getCombinedSketch();
            double width = combined.getMaxAngle() - combined.getMinAngle();
            if (!(width > 0) || Double.isNaN(width) || Double.isInfinite(width)) {
                continue;
            }
            survival *= 1.0 - combined.boundOvershoot(f);
            n++;
        }
        return n > 0 ? 1.0 - survival : 0.0;
    }


    public double getBoundOvershot(){
        return getBoundFalseProbability();
    }

    /**
     * Strict-verify this match against an exact-sketch list (e.g. OLSSketch instances
     * with zero bound width) that spans the same timeUnit grid as the match's source
     * sketches. Returns true iff each segment's filter contains the angle of the
     * combined exact sketches over that segment's time range.
     *
     * <p>Used by the cached pattern path after refinement: matches that the
     * ApproxOLS bound still classifies as ambiguous can be promoted to confident
     * without re-running the NFA, by combining the exact sketches that line up
     * with each segment and re-checking the filter rigorously.
     */
    public boolean verifyAgainstExact(List<Sketch> exactSketches,
                                      long alignedFrom,
                                      long timeUnitMillis) {
        if (exactSketches == null || exactSketches.isEmpty() || timeUnitMillis <= 0) {
            return false;
        }
        for (MatchSegment seg : segments) {
            ValueFilter filter = seg.getValueFilter();
            if (filter == null || filter.isValueAny()) {
                continue;
            }
            int startIdx = (int) ((seg.getStartTime() - alignedFrom) / timeUnitMillis);
            int endIdx = (int) ((seg.getEndTime() - alignedFrom) / timeUnitMillis);
            if (startIdx < 0 || endIdx > exactSketches.size() || startIdx >= endIdx) {
                return false;
            }
            List<Sketch> segSketches = exactSketches.subList(startIdx, endIdx);
            for (Sketch s : segSketches) {
                if (!s.hasInitialized()) {
                    return false;
                }
            }
            Sketch combined = SketchUtils.combineSketches(segSketches);
            if (!combined.matches(filter)) {
                return false;
            }
        }
        return true;
    }

}
