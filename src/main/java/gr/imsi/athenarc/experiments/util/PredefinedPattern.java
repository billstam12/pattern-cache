package gr.imsi.athenarc.experiments.util;

import gr.imsi.athenarc.middleware.query.pattern.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for managing predefined pattern templates with unique IDs
 */
public class PredefinedPattern {
    private final int id;
    private final String name;
    private final String description;
    private final List<PatternNode> patternNodes;
    
    private static final Map<Integer, PredefinedPattern> PATTERNS_BY_ID = new HashMap<>();
    
    public PredefinedPattern(int id, String name, String description, List<PatternNode> patternNodes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.patternNodes = patternNodes;
    }
    
    public int getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
    
    public List<PatternNode> getPatternNodes() {
        return new ArrayList<>(patternNodes);
    }
    
    /**
     * Initialize and register all predefined patterns
     */
    public static void initializePredefinedPatterns() {
        if (!PATTERNS_BY_ID.isEmpty()) {
            return; // Already initialized
        }
        
        int low = 20;
        int high = 25;
        // Pattern 1: Decreasing followed by increasing (V-shape)
        List<PatternNode> pattern2 = new ArrayList<>();
        pattern2.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeDecrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        pattern2.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeIncrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        registerPattern(new PredefinedPattern(1, "V-Shape", "A V-shaped pattern of decreasing followed by increasing", pattern2));
        
        // Pattern 2: Stable plateau /-\
        List<PatternNode> pattern3 = new ArrayList<>();
        pattern3.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeIncrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        pattern3.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.stable()
            ),
            RepetitionFactor.exactly(1)
        ));
        pattern3.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeDecrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        registerPattern(new PredefinedPattern(2, "Plateau", "A stable plateau with increasing and decreasing edges", pattern3));
        
        // Pattern 3: Oscillating pattern
        List<PatternNode> upDown = new ArrayList<>();
        upDown.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeIncrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        upDown.add(new SingleNode(
            new SegmentSpecification(
                new TimeFilter(false, low, high),
                ValueFilter.largeDecrease()
            ),
            RepetitionFactor.exactly(1)
        ));
        List<PatternNode> pattern4 = new ArrayList<>();
        pattern4.add(new GroupNode(upDown, RepetitionFactor.range(1, 2)));
        registerPattern(new PredefinedPattern(3, "Oscillating", "An oscillating pattern of repeated up-down movements", pattern4));
    }
    
    private static void registerPattern(PredefinedPattern pattern) {
        PATTERNS_BY_ID.put(pattern.getId(), pattern);
    }
    
    /**
     * Get a pattern by its ID
     * @param id the pattern ID
     * @return the PredefinedPattern or null if not found
     */
    public static PredefinedPattern getPatternById(int id) {
        return PATTERNS_BY_ID.get(id);
    }
    
    /**
     * Get all available pattern IDs
     * @return list of pattern IDs
     */
    public static List<Integer> getAllPatternIds() {
        return new ArrayList<>(PATTERNS_BY_ID.keySet());
    }
}
