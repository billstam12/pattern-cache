package gr.imsi.athenarc.middleware.pattern;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to compare pattern match results from different sources.
 * Can compare a match file against a ground truth file.
 */
public class PatternMatchComparator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternMatchComparator.class);
    private static final String MATCH_LOG_DIR = "pattern_match_logs";
    private static final String COMPARISON_DIR = "pattern_comparisons";

    // Class to represent a pattern match segment
    static class MatchSegment {
        long start;
        long end;
        Map<String, Double> attributes = new HashMap<>();
        
        @Override
        public String toString() {
            return "MatchSegment [start=" + start + ", end=" + end + ", attributes=" + attributes + "]";
        }
        
        /**
         * Check if two match segments are similar within a given tolerance
         */
        public boolean isSimilarTo(MatchSegment other, double tolerance) {
            // Time-range similarity
            long timeOverlap = Math.min(this.end, other.end) - Math.max(this.start, other.start);
            long timeUnion = Math.max(this.end, other.end) - Math.min(this.start, other.start);
            
            if (timeOverlap <= 0) {
                return false; // No time overlap
            }
            
            double timeScore = (double) timeOverlap / timeUnion;
            if (timeScore < tolerance) {
                return false;
            }
            
            // Value similarity - only compare attributes that both have
            if (!this.attributes.isEmpty() && !other.attributes.isEmpty()) {
                double valueSimilaritySum = 0.0;
                int valueCount = 0;
                
                for (String key : this.attributes.keySet()) {
                    if (other.attributes.containsKey(key)) {
                        double val1 = this.attributes.get(key);
                        double val2 = other.attributes.get(key);
                        
                        // Avoid division by zero
                        double maxVal = Math.max(Math.abs(val1), Math.abs(val2));
                        if (maxVal < 0.000001) {
                            valueSimilaritySum += 1.0; // If both values are essentially zero, they're identical
                        } else {
                            double diff = Math.abs(val1 - val2) / maxVal;
                            valueSimilaritySum += (1.0 - Math.min(diff, 1.0));
                        }
                        valueCount++;
                    }
                }
                
                if (valueCount > 0) {
                    double valueScore = valueSimilaritySum / valueCount;
                    if (valueScore < tolerance) {
                        return false;
                    }
                }
            }
            
            return true;
        }
    }

    // Class to represent a complete pattern match
    static class Match {
        List<MatchSegment> segments = new ArrayList<>();
        
        @Override
        public String toString() {
            return "Match [segments=" + segments + "]";
        }
        
        /**
         * Calculate similarity score with another match pattern
         * Returns a value between 0 (no similarity) and 1 (identical)
         */
        public double similarityScore(Match other, double tolerance) {
            if (this.segments.size() != other.segments.size()) {
                return 0.0; // Different segment count means they're not comparable
            }
            
            double totalScore = 0.0;
            for (int i = 0; i < this.segments.size(); i++) {
                MatchSegment segment1 = this.segments.get(i);
                MatchSegment segment2 = other.segments.get(i);
                
                if (segment1.isSimilarTo(segment2, tolerance)) {
                    totalScore += 1.0;
                }
            }
            
            return totalScore / this.segments.size();
        }
    }

    /**
     * Parse match file and extract match data
     */
    public static List<Match> parseMatchFile(String filePath) throws IOException {
        List<Match> matches = new ArrayList<>();
        Match currentMatch = null;
        MatchSegment currentSegment = null;
        
        // Updated patterns to match the new log format
        Pattern matchStartPattern = Pattern.compile("Match #(\\d+):(?: \\[(\\d+) to (\\d+)\\])?");
        Pattern segmentStartPattern = Pattern.compile("\\s+Segment (\\d+):(?: (.*))?");
        
        // Support both old and new time range formats
        Pattern timeRangePattern1 = Pattern.compile("\\s+Start: (\\d+), End: (\\d+)");
        Pattern timeRangePattern2 = Pattern.compile("\\s+\\[(\\d+) to (\\d+)\\]");
        
        Pattern attributePattern = Pattern.compile("\\s+([A-Za-z]+): ([0-9.-]+)");
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matchMatcher = matchStartPattern.matcher(line);
                if (matchMatcher.matches()) {
                    currentMatch = new Match();
                    matches.add(currentMatch);
                    
                    // If the match line contains the time range, remember it
                    if (matchMatcher.groupCount() >= 3 && matchMatcher.group(2) != null && matchMatcher.group(3) != null) {
                        // This will be used as a fallback if segments don't have time ranges
                        long matchStart = Long.parseLong(matchMatcher.group(2));
                        long matchEnd = Long.parseLong(matchMatcher.group(3));
                        LOG.debug("Found match with time range: {} to {}", matchStart, matchEnd);
                    }
                    continue;
                }
                
                if (currentMatch != null) {
                    Matcher segmentMatcher = segmentStartPattern.matcher(line);
                    if (segmentMatcher.matches()) {
                        currentSegment = new MatchSegment();
                        currentMatch.segments.add(currentSegment);
                        continue;
                    }
                    
                    if (currentSegment != null) {
                        // Try both time range formats
                        Matcher timeRangeMatcher1 = timeRangePattern1.matcher(line);
                        if (timeRangeMatcher1.matches()) {
                            currentSegment.start = Long.parseLong(timeRangeMatcher1.group(1));
                            currentSegment.end = Long.parseLong(timeRangeMatcher1.group(2));
                            continue;
                        }
                        
                        Matcher timeRangeMatcher2 = timeRangePattern2.matcher(line);
                        if (timeRangeMatcher2.matches()) {
                            currentSegment.start = Long.parseLong(timeRangeMatcher2.group(1));
                            currentSegment.end = Long.parseLong(timeRangeMatcher2.group(2));
                            continue;
                        }
                        
                        // Try to parse attribute values if any
                        Matcher attributeMatcher = attributePattern.matcher(line);
                        if (attributeMatcher.matches()) {
                            String attrName = attributeMatcher.group(1);
                            double attrValue = Double.parseDouble(attributeMatcher.group(2));
                            currentSegment.attributes.put(attrName, attrValue);
                        }
                    }
                }
            }
        }
        
        // Print some debug information about what was parsed
        LOG.info("Parsed {} matches from file {}", matches.size(), filePath);
        for (int i = 0; i < Math.min(matches.size(), 3); i++) {
            Match match = matches.get(i);
            LOG.info("Match #{} has {} segments", i + 1, match.segments.size());
            for (int j = 0; j < match.segments.size(); j++) {
                MatchSegment segment = match.segments.get(j);
                LOG.info("  Segment #{}: {} to {}", j, segment.start, segment.end);
            }
        }
        
        return matches;
    }

    /**
     * Compare two match result files and compute similarity metrics
     * 
     * @param groundTruthFile Path to the ground truth pattern match file
     * @param testFile Path to the test pattern match file to compare against ground truth
     * @param tolerance Similarity tolerance (0-1) for considering matches as similar
     * @return Map of metrics including precision, recall, and F1-score
     */
    public static Map<String, Double> compareMatchFiles(String groundTruthFile, String testFile, double tolerance) {
        Map<String, Double> metrics = new HashMap<>();
        
        try {
            // Parse files
            List<Match> groundTruthMatches = parseMatchFile(groundTruthFile);
            List<Match> testMatches = parseMatchFile(testFile);
            
            LOG.info("Ground truth contains {} matches", groundTruthMatches.size());
            LOG.info("Test file contains {} matches", testMatches.size());
            
            // For each ground truth match, find the most similar test match
            int truePositives = 0;
            List<Match> matchedTestMatches = new ArrayList<>();
            
            for (Match groundTruthMatch : groundTruthMatches) {
                double bestScore = 0.0;
                Match bestMatch = null;
                
                for (Match testMatch : testMatches) {
                    if (matchedTestMatches.contains(testMatch)) {
                        continue; // Skip already matched test patterns
                    }
                    
                    double score = groundTruthMatch.similarityScore(testMatch, tolerance);
                    if (score > bestScore && score >= tolerance) {
                        bestScore = score;
                        bestMatch = testMatch;
                    }
                }
                
                if (bestMatch != null) {
                    truePositives++;
                    matchedTestMatches.add(bestMatch);
                    LOG.debug("Match found with similarity score: {}", bestScore);
                }
            }
            
            // Calculate metrics
            int falsePositives = testMatches.size() - truePositives;
            int falseNegatives = groundTruthMatches.size() - truePositives;
            
            double precision = truePositives > 0 ? (double) truePositives / (truePositives + falsePositives) : 0;
            double recall = truePositives > 0 ? (double) truePositives / (truePositives + falseNegatives) : 0;
            double f1Score = (precision + recall > 0) ? 2 * precision * recall / (precision + recall) : 0;
            
            metrics.put("precision", precision);
            metrics.put("recall", recall);
            metrics.put("f1Score", f1Score);
            metrics.put("truePositives", (double) truePositives);
            metrics.put("falsePositives", (double) falsePositives);
            metrics.put("falseNegatives", (double) falseNegatives);
            
            // Generate comparison report
            generateComparisonReport(groundTruthFile, testFile, metrics, tolerance);
            
            return metrics;
            
        } catch (IOException e) {
            LOG.error("Error comparing match files", e);
            metrics.put("error", 1.0);
            return metrics;
        }
    }
    
    /**
     * Generate a comparison report between two pattern match files
     */
    private static void generateComparisonReport(String groundTruthFile, String testFile, 
                                               Map<String, Double> metrics, double tolerance) {
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(COMPARISON_DIR));
            
            // Create report file
            String groundTruthName = new File(groundTruthFile).getName();
            String testFileName = new File(testFile).getName();
            String reportFileName = String.format("comparison_%s_vs_%s.txt", 
                groundTruthName.replaceAll("\\.log$", ""), 
                testFileName.replaceAll("\\.log$", ""));
            
            File reportFile = new File(COMPARISON_DIR, reportFileName);
            
            try (FileWriter writer = new FileWriter(reportFile)) {
                writer.write("Pattern Match Comparison Report\n");
                writer.write("============================\n\n");
                
                writer.write("Ground Truth File: " + groundTruthFile + "\n");
                writer.write("Test File: " + testFile + "\n");
                writer.write("Similarity Tolerance: " + tolerance + "\n\n");
                
                writer.write("Metrics:\n");
                writer.write(String.format("- Precision: %.4f\n", metrics.get("precision")));
                writer.write(String.format("- Recall: %.4f\n", metrics.get("recall")));
                writer.write(String.format("- F1 Score: %.4f\n", metrics.get("f1Score")));
                writer.write(String.format("- True Positives: %.0f\n", metrics.get("truePositives")));
                writer.write(String.format("- False Positives: %.0f\n", metrics.get("falsePositives")));
                writer.write(String.format("- False Negatives: %.0f\n", metrics.get("falseNegatives")));
            }
            
            LOG.info("Comparison report generated: {}", reportFile.getAbsolutePath());
            
        } catch (IOException e) {
            LOG.error("Failed to generate comparison report", e);
        }
    }
    
    /**
     * Main method for command-line usage of the comparator
     */
    public static void main(String[] args) {
        // if (args.length < 2) {
        //     System.out.println("Usage: PatternMatchComparator <groundTruthFile> <testFile> [tolerance]");
        //     System.out.println("Example:");
        //     System.out.println("  java -cp pattern-cache.jar gr.imsi.athenarc.middleware.pattern.PatternMatchComparator");
        //     System.out.println("       pattern_match_logs/direct_pattern_query_20230615_120000_1_MINUTE.log");
        //     System.out.println("       pattern_match_logs/cached_pattern_query_20230615_120000_1_MINUTE.log 0.8");
        //     return;
        // }
        // String groundTruthFile = args[0];
        // String testFile = args[1];
        // double tolerance = args.length > 2 ? Double.parseDouble(args[2]) : 0.8;

        String groundTruthFile = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/PhD/Code/pattern-cache/pattern_match_logs/cached_pattern_query_20250611_144251_1_AggregateInterval{1 Days}.log";
        String testFile = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/PhD/Code/pattern-cache/pattern_match_logs/cached_pattern_query_20250611_144251_1_AggregateInterval{1 Days}.log";
        double tolerance = 1;
        
        Map<String, Double> metrics = compareMatchFiles(groundTruthFile, testFile, tolerance);
        
        System.out.println("Comparison Metrics:");
        System.out.printf("- Precision: %.4f\n", metrics.get("precision"));
        System.out.printf("- Recall: %.4f\n", metrics.get("recall"));
        System.out.printf("- F1 Score: %.4f\n", metrics.get("f1Score"));
    }
}
