package gr.imsi.athenarc.experiments.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for loading state transition configurations
 */
public class StateTransitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateTransitionLoader.class);
    
    /**
     * Load state transitions from a file
     * @param filePath Path to the properties file containing state transitions
     * @return Properties object with the loaded transitions
     */
    public static Properties loadStateTransitions(String filePath) {
        Properties properties = new Properties();
        
        try {
            File file = new File(filePath);
            if (file.exists()) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    properties.load(fis);
                    LOG.info("Loaded state transitions from: {}", filePath);
                }
            } else {
                // Try loading from classpath
                try (InputStream is = StateTransitionLoader.class.getResourceAsStream("/" + filePath)) {
                    if (is != null) {
                        properties.load(is);
                        LOG.info("Loaded state transitions from classpath: {}", filePath);
                    } else {
                        LOG.warn("State transition file not found: {}", filePath);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to load state transitions from: " + filePath, e);
        }
        
        return properties;
    }
    
    /**
     * Create a state transition matrix from properties
     * @param properties Properties containing transition probabilities
     * @return State transition matrix
     */
    public static Map<UserOpType, Map<UserOpType, Double>> createStateTransitionMatrix(Properties properties) {
        Map<UserOpType, Map<UserOpType, Double>> matrix = new HashMap<>();
        
        // Initialize the matrix with empty maps for all states
        for (UserOpType state : UserOpType.values()) {
            matrix.put(state, new HashMap<>());
        }
        
        // Look for transition probabilities in the properties
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("transition.")) {
                String[] parts = key.split("\\.");
                if (parts.length == 3) {
                    try {
                        UserOpType fromState = UserOpType.valueOf(parts[1]);
                        UserOpType toState = UserOpType.valueOf(parts[2]);
                        double probability = Double.parseDouble(properties.getProperty(key));
                        
                        Map<UserOpType, Double> transitions = matrix.get(fromState);
                        transitions.put(toState, probability);
                        
                        LOG.debug("Added transition: {} -> {} = {}", fromState, toState, probability);
                    } catch (IllegalArgumentException e) {
                        LOG.warn("Invalid state transition property: {}", key);
                    }
                }
            }
        }
        
        LOG.info("Created state transition matrix with {} states", matrix.size());
        return matrix;
    }
    
    /**
     * Configure a QuerySequenceGenerator with state transitions from a file
     * @param generator The generator to configure
     * @param filePath Path to the properties file
     */
    public static void configureGenerator(QuerySequenceGenerator generator, String filePath) {
        Properties properties = loadStateTransitions(filePath);
        generator.setProbabilitiesFromProperties(properties);
        generator.setStateTransitionsFromProperties(properties);
    }
    
    /**
     * Validate that transition probabilities from a state sum to at most 1.0
     * @param matrix The state transition matrix to validate
     * @return True if the matrix is valid, false otherwise
     */
    public static boolean validateTransitionMatrix(Map<UserOpType, Map<UserOpType, Double>> matrix) {
        boolean valid = true;
        
        for (Map.Entry<UserOpType, Map<UserOpType, Double>> entry : matrix.entrySet()) {
            UserOpType fromState = entry.getKey();
            Map<UserOpType, Double> transitions = entry.getValue();
            
            double sum = transitions.values().stream().mapToDouble(Double::doubleValue).sum();
            if (sum > 1.01) { // Allow for small floating point errors
                LOG.warn("Transition probabilities from state {} sum to {}, exceeding 1.0", fromState, sum);
                valid = false;
            }
        }
        
        return valid;
    }
}
