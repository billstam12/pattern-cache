package gr.imsi.athenarc.middleware.pattern;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
     * Creates a new PatternMatch from a list of sketch segments.
     * 
     * @param sketchSegments The segments (each segment is a list of sketches)
     */
    public PatternMatch(List<List<Sketch>> sketchSegments) {
        if (sketchSegments == null || sketchSegments.isEmpty()) {
            throw new IllegalArgumentException("Match must contain at least one segment");
        }
        
        this.segments = new ArrayList<>();
        
        // Create MatchSegment objects
        for (int i = 0; i < sketchSegments.size(); i++) {
            segments.add(new MatchSegment(sketchSegments.get(i), i));
        }
        
        // Calculate match statistics
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
        // Write match summary
        writer.write(String.format("Match : [%d to %d] - Average Error Margin: %.2f%% (%d segments, %d total sketches)\n", 
            startTime, endTime, averageErrorMargin * 100.0, segments.size(), matchLength));
        
        // Write segment details
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
        
        /**
         * Creates a new MatchSegment from a list of sketches.
         * 
         * @param sketches The sketches that form this segment
         * @param segmentIndex The index of this segment within its parent match
         */
        public MatchSegment(List<Sketch> sketches, int segmentIndex) {
            if (sketches == null || sketches.isEmpty()) {
                throw new IllegalArgumentException("Segment must contain at least one sketch");
            }
            
            this.sketches = sketches;
            this.segmentIndex = segmentIndex;
            
            // Calculate time bounds
            Sketch combinedSketch = SketchUtils.combineSketches(sketches);
            this.startTime = combinedSketch.getFrom();
            this.endTime = combinedSketch.getTo();
            
            // Calculate error margin
            double segmentError = combinedSketch.getAngleErrorMargin();
            this.errorMargin = (!Double.isNaN(segmentError) && !Double.isInfinite(segmentError)) 
                ? segmentError : 0.0;
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
            return SketchUtils.combineSketches(sketches);
        }
        
        @Override
        public String toString() {
            return String.format("MatchSegment[%d]: [%d to %d] - Error Margin: %.2f%% (%d sketches)", 
                segmentIndex, startTime, endTime, errorMargin * 100.0, sketches.size());
        }
    }
}
