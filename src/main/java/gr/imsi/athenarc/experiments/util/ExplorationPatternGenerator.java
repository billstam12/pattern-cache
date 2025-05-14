package gr.imsi.athenarc.experiments.util;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Utility for generating query sequences based on different exploration patterns
 */
public class ExplorationPatternGenerator {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExplorationPatternGenerator.class);
        

    /**
     * Load all available exploration patterns from a specified file
     * @param patternFile Path to the pattern file
     * @return Map of pattern names to their property sets
     */
    public static Properties loadExplorationPatterns(String patternName) {
        Properties patternProps = new Properties();
        
        String patternFile = "";
        switch (patternName) {
            case "DRILL-DOWN EXPLORER":
                patternFile = "/patterns/drill_down_explorer.properties";
                break;
            case "PATTERN HUNTER":
                patternFile = "/patterns/pattern_hunter.properties";
                break;
            case "OVERVIEW EXPLORER":
                patternFile = "/patterns/overview_explorer.properties";
                break;
            case "MULTI-MEASURE ANALYST":
                patternFile = "/patterns/multi_measure_analyst.properties";
                break;
            default:
                patternFile = "/patterns/state-transitions.properties";
                break;
        }
        try {
            // Try loading from file system first
            // If not found, try classpath
            InputStream is = ExplorationPatternGenerator.class.getResourceAsStream(patternFile);
            if (is != null) {
                patternProps.load(is);
                is.close();
            } else {
                LOG.error("Could not load exploration patterns file: {}", patternFile);
                return patternProps;
            }

            
        } catch (IOException e) {
            LOG.error("Error loading exploration patterns", e);
        }
        
        return patternProps;
    }
    
    /**
     * Generate a query sequence using a specific exploration pattern
     * @param dataset The dataset to query
     * @param patternName Name of the pattern to use
     * @param startQuery Initial query
     * @param queryCount Number of queries to generate
     * @return List of generated queries
     */
    public static List<TypedQuery> generatePatternedQuerySequence(
            AbstractDataset dataset, String patternName, Query startQuery, int queryCount) {
        
        Properties patternProps = loadExplorationPatterns(patternName);
        
        QuerySequenceGenerator generator = new QuerySequenceGenerator(dataset);
        generator.setProbabilitiesFromProperties(patternProps);
        generator.setStateTransitionsFromProperties(patternProps);
        
        return generator.generateQuerySequence(startQuery, queryCount);
    }
}
