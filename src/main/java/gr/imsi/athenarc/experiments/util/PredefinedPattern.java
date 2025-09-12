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
        
        // Patterns for (5,5), (15,15), (30,30) with descriptive names
        int[] values = {5, 15, 25};
        String[] sizeNames = {"small", "mid", "big"};
        int id = 1;
        for (int i = 0; i < values.length; i++) {
            int val = values[i];
            String size = sizeNames[i];

            // v_shape
            List<PatternNode> vShape = new ArrayList<>();
            vShape.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeDecrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            vShape.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeIncrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            registerPattern(new PredefinedPattern(id++, "v_shape_" + size, "V-shape, low=high=" + val, vShape));

            // plateau
            List<PatternNode> plateau = new ArrayList<>();
            plateau.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeIncrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            plateau.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.stable()
                ),
                RepetitionFactor.exactly(1)
            ));
            plateau.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeDecrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            registerPattern(new PredefinedPattern(id++, "plateau_" + size, "Plateau, low=high=" + val, plateau));

            // oscillating
            List<PatternNode> oscillating = new ArrayList<>();
            oscillating.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeIncrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            oscillating.add(new SingleNode(
                new SegmentSpecification(
                    new TimeFilter(false, val, val),
                    ValueFilter.largeDecrease()
                ),
                RepetitionFactor.exactly(1)
            ));
            registerPattern(new PredefinedPattern(id++, "oscillating_" + size, "Oscillating, low=high=" + val, oscillating));
        }
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
