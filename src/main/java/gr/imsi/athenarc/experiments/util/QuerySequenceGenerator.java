package gr.imsi.athenarc.experiments.util;

import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.pattern.*;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryBuilder;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.domain.ViewPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static gr.imsi.athenarc.experiments.util.UserOpType.*;

public class QuerySequenceGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySequenceGenerator.class);

    private AbstractDataset dataset;

    private UserOpType opType;
    
    int seed = 42;
    
    // Main random generator
    private Random mainRandom;
    
    double RESIZE_FACTOR = 1.2;
    
    // Configurable operation probabilities
    private float minShift = 0f;
    private float maxShift = 0.5f;

    private double maxZoomFactor = 0.5;
    private double panProbability = 0.5;
    private double zoomInProbability = 0.2;
    private double zoomOutProbability = 0.2;
    private double measureChangeProbability = 0.0;
    private double resizeProbability = 0.0;
    private double patternDetectionProbability = 0.1;
    
    // State transition related fields
    private boolean useStateTransitions = false;
    private Map<UserOpType, Map<UserOpType, Double>> stateTransitionMatrix;
    private UserOpType currentState = null;

    public QuerySequenceGenerator(AbstractDataset dataset) {
        this.dataset = dataset;
        this.mainRandom = new Random(seed);
        this.useStateTransitions = false;
        // Initialize predefined patterns
        PredefinedPattern.initializePredefinedPatterns();
        initStateTransitionMatrix();
        
    }
    
    /**
     * Constructor with configurable operation probabilities
     */
    public QuerySequenceGenerator(AbstractDataset dataset,
                                 double panProbability, 
                                 double zoomInProbability, double zoomOutProbability,
                                 double resizeProbability, double measureChangeProbability, 
                                 double patternDetectionProbability) {
        this(dataset);
        this.panProbability = panProbability;
        this.zoomInProbability = zoomInProbability;
        this.zoomOutProbability = zoomOutProbability;
        this.resizeProbability = resizeProbability;
        this.measureChangeProbability = measureChangeProbability;
        this.patternDetectionProbability = patternDetectionProbability;
    }
    
    /**
     * Constructor with state transition matrix
     */
    public QuerySequenceGenerator(AbstractDataset dataset, 
                                 Map<UserOpType, Map<UserOpType, Double>> stateTransitionMatrix) {
        this(dataset);
        this.stateTransitionMatrix = stateTransitionMatrix;
        this.useStateTransitions = true;
        LOG.info("Using state transitions for query generation");
    }
    
    /**
     * Initialize the state transition matrix with default values
     */
    private void initStateTransitionMatrix() {
        stateTransitionMatrix = new HashMap<>();
        
        // Initialize empty transition probabilities for each state
        for (UserOpType fromState : UserOpType.values()) {
            Map<UserOpType, Double> transitions = new HashMap<>();
            stateTransitionMatrix.put(fromState, transitions);
            
            // Initialize all transition probabilities to 0
            for (UserOpType toState : UserOpType.values()) {
                transitions.put(toState, 0.0);
            }
        }
        
        LOG.debug("Initialized state transition matrix with all probabilities set to 0");
    }
    
    /**
     * Set transition probability from one state to another
     * @param fromState The starting state
     * @param toState The destination state
     * @param probability The transition probability
     */
    private void setStateTransitionProbability(UserOpType fromState, UserOpType toState, double probability) {
        Map<UserOpType, Double> transitions = stateTransitionMatrix.get(fromState);
        if (transitions == null) {
            transitions = new HashMap<>();
            stateTransitionMatrix.put(fromState, transitions);
        }
        transitions.put(toState, probability);
        
        // Enable state transitions when probabilities are set
        if (probability > 0) {
            useStateTransitions = true;
        }
        
        LOG.debug("Set transition probability from {} to {}: {}", fromState, toState, probability);
    }
    
    /**
     * Set state transition probabilities from properties
     * @param properties Properties containing transition probabilities
     */
    public void setStateTransitionsFromProperties(Properties properties) {
        if (properties == null) return;
        
        // Look for transition probabilities in the format: "transition.FROM.TO=probability"
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("transition.")) {
                String[] parts = key.split("\\.");
                if (parts.length == 3) {
                    try {
                        UserOpType fromState = UserOpType.valueOf(parts[1]);
                        UserOpType toState = UserOpType.valueOf(parts[2]);
                        
                        // Get the property value and extract just the probability
                        String propertyValue = properties.getProperty(key);
                        // Extract numeric part (in case there are comments after the value)
                        String probabilityStr = propertyValue.split("\\s+")[0];
                        double probability = Double.parseDouble(probabilityStr);
                        
                        setStateTransitionProbability(fromState, toState, probability);
                    } catch (IllegalArgumentException e) {
                        LOG.warn("Invalid state transition property: {} - {}", key, e.getMessage());
                    }
                }
            }
        }
        LOG.info("Configured state transitions from properties: {}", stateTransitionMatrix);
    }
    
    /**
     * Get the next state based on current state and transition probabilities
     * @param currentState The current state
     * @return The next state based on transition probabilities
     */
    private UserOpType getNextState(UserOpType currentState) {
        if (currentState == null) {
            // For initial state, choose based on configured probabilities
            return randomStateFromProbabilities();
        }
        
        Map<UserOpType, Double> transitions = stateTransitionMatrix.get(currentState);
        if (transitions == null || transitions.isEmpty()) {
            // Fall back to random selection if no transitions defined
            return randomStateFromProbabilities();
        }
        
        return MarkovUtils.pickRandomOp(transitions);
    }
    
    /**
     * Generate a random state based on configured operation probabilities
     */
    private UserOpType randomStateFromProbabilities() {
        List<UserOpType> states = new ArrayList<>();
        List<Double> probs = new ArrayList<>();
        
        // Add states with their probabilities
        addStateWithProbability(states, probs, P, panProbability);
        addStateWithProbability(states, probs, ZI, zoomInProbability);
        addStateWithProbability(states, probs, ZO, zoomOutProbability);
        addStateWithProbability(states, probs, R, resizeProbability);
        addStateWithProbability(states, probs, MC, measureChangeProbability);
        addStateWithProbability(states, probs, PD, patternDetectionProbability);
        
        // If no valid probabilities, default to PAN
        if (states.isEmpty()) {
            return P;
        }
        
        // Calculate cumulative probabilities
        double total = probs.stream().mapToDouble(Double::doubleValue).sum();
        double rand = mainRandom.nextDouble() * total;
        double cumulative = 0;
        
        for (int i = 0; i < states.size(); i++) {
            cumulative += probs.get(i);
            if (rand <= cumulative) {
                return states.get(i);
            }
        }
        
        // Default to first state if something went wrong
        return states.get(0);
    }
    
    /**
     * Helper method to add a state with its probability to the lists
     */
    private void addStateWithProbability(List<UserOpType> states, List<Double> probs, UserOpType state, double probability) {
        if (probability > 0) {
            states.add(state);
            probs.add(probability);
        }
    }

    public void addRandomElementToList(List<Integer> list1, List<Integer> list2) {
        Random random = new Random(mainRandom.nextInt());

        while (true) {
            // Generate a random index to select an element from list2
            int randomIndex = random.nextInt(list2.size());

            // Get the random element from list2
            Integer randomElement = list2.get(randomIndex);

            // Check if the element is not in list1
            if (!list1.contains(randomElement)) {
                // If it's not in list1, add it and exit the loop
                list1.add(randomElement);
                LOG.debug("Added element: " + randomElement);
                break;
            }
        }
    }

    private int getViewportId(List<ViewPort>  viewPorts, ViewPort viewPort){
         int idx = 0;
         for(ViewPort v : viewPorts){
             if(v.getWidth() == viewPort.getWidth() && v.getHeight() == v.getHeight()) break;
             idx ++;
         }
         return idx;
    }

    public List<TypedQuery> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
        Random shiftRandom = new Random(mainRandom.nextLong());
        double[] shifts = shiftRandom.doubles(count, minShift, maxShift).toArray();
        
        Random zoomRandom = new Random(mainRandom.nextLong());
        double[] zoomInFactors = zoomRandom.doubles(count, maxZoomFactor, 1).toArray();
        double[] zoomOutFactors = zoomRandom.doubles(count, 1, 1 + maxZoomFactor).toArray();

        Random opRandom = new Random(mainRandom.nextLong());
        Random measureChangeRandom = new Random(mainRandom.nextLong());
        
        List<UserOpType> ops = new ArrayList<>();

        if (!useStateTransitions) {
            // Calculate the number of operations based on probabilities (original approach)
            int total = count;
            int pans = (int) (total * panProbability);
            int zoom_out = (int) (total * zoomOutProbability);
            int zoom_in = (int) (total * zoomInProbability);
            int resize = (int) (total * resizeProbability);
            int measure_change = (int) (total * measureChangeProbability);

            // Make sure we have at least one operation of each type
            int remaining = total - (pans + zoom_out + zoom_in + resize + measure_change);
            if (remaining > 0) {
                pans += remaining; // Add remaining to pans
            }

            for (int i = 0; i < pans; i++) ops.add(P);
            for (int i = 0; i < zoom_in; i++) ops.add(ZI);
            for (int i = 0; i < zoom_out; i++) ops.add(ZO);
            for (int i = 0; i < resize; i++) ops.add(R);
            for (int i = 0; i < measure_change; i++) ops.add(MC);
        } else {
            // Generate operations based on state transitions
            currentState = null; // Start with no state
            for (int i = 0; i < count; i++) {
                currentState = getNextState(currentState);
                ops.add(currentState);
            }
        }

        List<TypedQuery> queries = new ArrayList<>();
        Query q = q0;
        TypedQuery typedQuery = new TypedQuery(q, null, -1);
        queries.add(typedQuery);

        List<ViewPort> viewPorts = new ArrayList<>();
        viewPorts.add(new ViewPort(500, 250));
        viewPorts.add(new ViewPort(1000, 500));
        viewPorts.add(new ViewPort(2000, 1000));

        for (int i = 0; i < count; i++) {
            // If using state transitions, take operation from predefined sequence
            // Otherwise, select randomly
            if (useStateTransitions) {
                opType = ops.get(i);
            } else {
                opType = ops.get(opRandom.nextInt(ops.size()));
            }
            
            TimeRange timeRange = new TimeRange(q.getFrom(), q.getTo());   
            ViewPort viewPort = q.getViewPort();
            List<Integer> measures = new ArrayList<>(q.getMeasures());
            // Handle measure change operation
            if (opType.equals(MC)) {
                boolean addMeasure = measureChangeRandom.nextBoolean();
                if (addMeasure) {
                    // Add a measure if not all measures are already included
                    if (measures.size() != dataset.getMeasures().size()) {
                        addRandomElementToList(measures, dataset.getMeasures());
                    }
                } else {
                    // Remove a measure if there's more than one
                    if (measures.size() > 1) {
                        int indexToRemove = measureChangeRandom.nextInt(measures.size());
                        measures.remove(indexToRemove);
                    }
                }
            }
            else if (opType.equals(ZI)) {
                timeRange = zoom(q, (float) zoomInFactors[i]);
            } else if (opType.equals(ZO)) {
                timeRange = zoom(q, (float) zoomOutFactors[i]);
            } else if (opType.equals(P)) {
                timeRange = pan(q, shifts[i], directions[i]);
            } else if (opType.equals(R)) {
                Random viewPortRandom = new Random(mainRandom.nextLong());
                while (true) {
                    int viewPortId = viewPortRandom.nextInt(viewPorts.size());
                    if (viewPortId != getViewportId(viewPorts, viewPort)) {
                        viewPort = viewPorts.get(viewPortId);
                        break;
                    }
                }
            } 
            // Pattern detection requires special handling
            int patternId = -1; // Default: not a pattern
            if(opType.equals(PD)){
                Random patternRandom = new Random(mainRandom.nextInt());
                // Generate pattern query and capture pattern ID
                typedQuery = generatePatternQuery(q, patternRandom);
                // q will be updated inside the method
            } else {
                // Generate a visual query
                q = new VisualQueryBuilder()
                    .withTimeRange(timeRange.getFrom(), timeRange.getTo())
                    .withMeasures(measures)
                    .withViewPort(viewPort.getWidth(), viewPort.getHeight())
                    .withAccuracy(q0.getAccuracy())
                    .build();
                typedQuery = new TypedQuery(q, opType, patternId);
            }
            queries.add(typedQuery);
            // Update current state if using state transitions
            if (useStateTransitions) {
                currentState = opType;
            }
        }
        return queries;
    }
    
    /**
     * Generates a pattern query with randomized pattern parameters
     * @param baseQuery The source query to build from
     * @param random Random generator
     * @return The ID of the pattern used
     */
    private TypedQuery generatePatternQuery(Query baseQuery, Random random) {
        long from = baseQuery.getFrom();
        long to = baseQuery.getTo();
        int measure = baseQuery.getMeasures().get(0);
        ViewPort viewPort = baseQuery.getViewPort();
        
        // Generate an aggregation interval smaller than the time range
        double level = random.nextInt(4) + 0.1; 
        AggregateInterval timeUnit = DateTimeUtil.roundDownToCalendarBasedInterval((long) Math.floor((to - from) / (level * viewPort.getWidth())));

        // Generate a random aggregation type
        AggregationType[] types = AggregationType.values();
        AggregationType aggregationType = types[random.nextInt(types.length)];
        
        // Choose a predefined pattern ID and get the pattern
        List<Integer> patternIds = PredefinedPattern.getAllPatternIds();
        int patternId = patternIds.get(random.nextInt(patternIds.size()));
        PredefinedPattern predefinedPattern = PredefinedPattern.getPatternById(patternId);
        
        // Create a pattern query using the builder
        PatternQuery q = new PatternQueryBuilder()
            .withTimeRange(from, to)
            .withMeasure(measure)
            .withTimeUnit(timeUnit)
            .withAggregationType(aggregationType)
            .withPatternNodes(predefinedPattern.getPatternNodes())
            .withViewPort(viewPort.getWidth(), viewPort.getHeight())
            .withAccuracy(baseQuery.getAccuracy())
            .build();
            
        return new TypedQuery(q, opType, patternId);
    }

    private TimeRange pan(Query query, double shift, Direction direction) {
        long from = query.getFrom();
        long to = query.getTo();
        long datasetStart = dataset.getTimeRange().getFrom();
        long datasetEnd = dataset.getTimeRange().getTo();
        long currentRange = to - from;
        
        // Calculate adjusted shift based on proximity to dataset boundaries
        long timeShift = (long) (currentRange * shift);
        
        switch (direction) {
            case L:
                // When panning left, check proximity to left boundary
                if (from - datasetStart < currentRange) {
                    // We're close to the left edge, scale down the shift
                    double proximityFactor = Math.max(0.1, (double)(from - datasetStart) / currentRange);
                    timeShift = (long)(timeShift * proximityFactor);
                }
                
                // Apply the shift with boundaries check
                from = Math.max(datasetStart, from - timeShift);
                to = from + currentRange;
                break;

            case R:
                // When panning right, check proximity to right boundary
                if (datasetEnd - to < currentRange) {
                    // We're close to the right edge, scale down the shift
                    double proximityFactor = Math.max(0.1, (double)(datasetEnd - to) / currentRange);
                    timeShift = (long)(timeShift * proximityFactor);
                }
                
                // Apply the shift with boundaries check
                to = Math.min(datasetEnd, to + timeShift);
                from = to - currentRange;
                break;
                
            default:
                return new TimeRange(from, to);
        }
        
        LOG.debug("Pan {} with shift {}: [{} -> {}]", 
            direction, timeShift, DateTimeUtil.format(from), DateTimeUtil.format(to));
            
        return new TimeRange(from, to);
    }

    private TimeRange zoom(Query query, float zoomFactor) {
        long from = query.getFrom();
        long to = query.getTo();
        float middle = (float) (from + to) / 2f;
        float size = (float) (to - from) * zoomFactor;
        long newFrom = (long) (middle - (size / 2f));
        long newTo = (long) (middle + (size / 2f));

        if (dataset.getTimeRange().getTo() < newTo) {
            newTo = dataset.getTimeRange().getTo();
        }
        if (dataset.getTimeRange().getFrom() > newFrom) {
            newFrom = dataset.getTimeRange().getFrom();
        }
        if (newFrom >= newTo) {
            return null;
        }

        return new TimeRange(newFrom, newTo);
    }
    
    public double getPanProbability() {
        return panProbability;
    }

    public void setPanProbability(double panProbability) {
        this.panProbability = panProbability;
    }

    public double getZoomInProbability() {
        return zoomInProbability;
    }

    public void setZoomInProbability(double zoomInProbability) {
        this.zoomInProbability = zoomInProbability;
    }

    public double getZoomOutProbability() {
        return zoomOutProbability;
    }

    public void setZoomOutProbability(double zoomOutProbability) {
        this.zoomOutProbability = zoomOutProbability;
    }

    public double getResizeProbability() {
        return resizeProbability;
    }

    public void setResizeProbability(double resizeProbability) {
        this.resizeProbability = resizeProbability;
    }
    
    public double getMeasureChangeProbability() {
        return measureChangeProbability;
    }

    public void setMeasureChangeProbability(double measureChangeProbability) {
        this.measureChangeProbability = measureChangeProbability;
    }

    /**
     * Sets all probability values from a Properties object
     * @param properties Properties containing configuration values
     */
    public void setProbabilitiesFromProperties(Properties properties) {
        if (properties == null) return;
        
        // Get probabilities from properties with default fallback
        panProbability = Double.parseDouble(properties.getProperty("panProbability", String.valueOf(panProbability)));
        zoomInProbability = Double.parseDouble(properties.getProperty("zoomInProbability", String.valueOf(zoomInProbability)));
        zoomOutProbability = Double.parseDouble(properties.getProperty("zoomOutProbability", String.valueOf(zoomOutProbability)));
        resizeProbability = Double.parseDouble(properties.getProperty("resizeProbability", String.valueOf(resizeProbability)));
        measureChangeProbability = Double.parseDouble(properties.getProperty("measureChangeProbability", String.valueOf(measureChangeProbability)));
        patternDetectionProbability = Double.parseDouble(properties.getProperty("patternDetectionProbability", String.valueOf(patternDetectionProbability)));

        if(patternDetectionProbability + panProbability + zoomInProbability + zoomOutProbability + resizeProbability + measureChangeProbability > 1){
            LOG.warn("Probabilities sum to more than 1. Setting them to default values.");
            panProbability = 0.5;
            zoomInProbability = 0.2;
            zoomOutProbability = 0.2;
            resizeProbability = 0.0;
            measureChangeProbability = 0.0;
            patternDetectionProbability = 0.1;
        }
    }
    
    /**
     * @return Whether state transitions are being used
     */
    public boolean isUsingStateTransitions() {
        return useStateTransitions;
    }
    
    /**
     * Enable or disable state transitions
     * @param useStateTransitions Whether to use state transitions
     */
    public void setUseStateTransitions(boolean useStateTransitions) {
        this.useStateTransitions = useStateTransitions;
    }
    
    /**
     * Get the current state transition matrix
     * @return The state transition matrix
     */
    public Map<UserOpType, Map<UserOpType, Double>> getStateTransitionMatrix() {
        return stateTransitionMatrix;
    }
    
    /**
     * Set the state transition matrix
     * @param stateTransitionMatrix The new state transition matrix
     */
    public void setStateTransitionMatrix(Map<UserOpType, Map<UserOpType, Double>> stateTransitionMatrix) {
        this.stateTransitionMatrix = stateTransitionMatrix;
        this.useStateTransitions = true;
    }

    /**
     * Generates a query sequence based on a previously saved query file
     * 
     * @param filePath Path to the file containing the saved queries
     * @return List of TypedQuery objects generated from the file
     */
    public List<TypedQuery> generateQuerySequenceFromFile(String filePath) {
        List<TypedQuery> queries = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                TypedQuery typedQuery = parseQueryFromLine(line);
                if (typedQuery != null) {
                    queries.add(typedQuery);
                }
            }
            LOG.info("Loaded {} queries from file: {}", queries.size(), filePath);
        } catch (IOException e) {
            LOG.error("Error reading queries from file: " + e.getMessage());
            throw new RuntimeException(e);
        }
        
        return queries;
    }
    
    /**
     * Parse a single line from the queries file and convert it to a TypedQuery object
     * 
     * @param line The line from the file to parse
     * @return A TypedQuery object representing the query in the line
     */
    private TypedQuery parseQueryFromLine(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length < 8) {
                LOG.warn("Invalid line format (too few fields): {}", line);
                return null;
            }
            
            // Parse timestamps
            long from = Long.parseLong(parts[0]);
            long to = Long.parseLong(parts[1]);
            
            // Parse measures (may contain multiple measures separated by |)
            String[] measureStrings = parts[4].split("\\|");
            List<Integer> measures = new ArrayList<>();
            for (String measureStr : measureStrings) {
                measures.add(Integer.parseInt(measureStr));
            }
            
            // Parse viewport dimensions
            int viewportWidth = Integer.parseInt(parts[5]);
            int viewportHeight = Integer.parseInt(parts[6]);
            
            // Parse accuracy
            double accuracy = Double.parseDouble(parts[7]);
            
            // Parse query type
            boolean isPattern = "pattern".equals(parts[8]);
            
            // Parse query operation type (if present)
            UserOpType opType = null;
            int patternId = -1;
            String opTypeStr = parts[9];
            opType = parseUserOpType(opTypeStr);
            
            // Create the appropriate query based on type
            Query query;
            if (isPattern) {
                // For pattern queries, we need to reconstruct the pattern
                patternId = getPatternIdByName(opTypeStr);
                String[] aggregateInterval = parts[10].split(" ");
                AggregateInterval timeUnit = AggregateInterval.of(Long.parseLong(aggregateInterval[0]), ChronoUnit.valueOf(aggregateInterval[1].toUpperCase()));
                PredefinedPattern predefinedPattern = PredefinedPattern.getPatternById(patternId);
                
                // Use default aggregate interval and type if not available in file
                AggregationType aggregationType = AggregationType.LAST_VALUE;
                
                query = new PatternQueryBuilder()
                    .withTimeRange(from, to)
                    .withMeasure(measures.get(0))
                    .withTimeUnit(timeUnit)
                    .withAggregationType(aggregationType)
                    .withPatternNodes(predefinedPattern.getPatternNodes())
                    .withViewPort(viewportWidth, viewportHeight)
                    .withAccuracy(accuracy)
                    .build();
            } else {
                // For visual queries
                query = new VisualQueryBuilder()
                    .withTimeRange(from, to)
                    .withMeasures(measures)
                    .withViewPort(viewportWidth, viewportHeight)
                    .withAccuracy(accuracy)
                    .build();
            }
            return new TypedQuery(query, opType, patternId);
        } catch (Exception e) {
            LOG.error("Error parsing query line: " + line, e);
            return null;
        }
    }
    
    /**
     * Parse the operation type from its string representation
     * 
     * @param opTypeStr String representation of the operation type
     * @return The UserOpType enum value
     */
    private UserOpType parseUserOpType(String opTypeStr) {
        // Handle the initial query case
        if ("initial".equals(opTypeStr)) {
            return null;
        }
        
        // Handle standard operation types
        switch (opTypeStr) {
            case "pan":
                return UserOpType.P;
            case "zoomIn":
                return UserOpType.ZI;
            case "zoomOut":
                return UserOpType.ZO;
            case "resize":
                return UserOpType.R;
            case "measureChange":
                return UserOpType.MC;
            default:
                // If not a standard operation type, it might be a pattern name
                if (getPatternIdByName(opTypeStr) != -1) {
                    return UserOpType.PD;
                }
                LOG.warn("Unknown operation type: {}", opTypeStr);
                return null;
        }
    }
    
    /**
     * Get a pattern ID based on its name
     * 
     * @param patternName The name of the pattern
     * @return The pattern ID or -1 if not found
     */
    private int getPatternIdByName(String patternName) {
        List<Integer> patternIds = PredefinedPattern.getAllPatternIds();
        
        for (Integer id : patternIds) {
            PredefinedPattern pattern = PredefinedPattern.getPatternById(id);
            if (pattern.getName().equalsIgnoreCase(patternName)) {
                return id;
            }
        }
        
        return -1;
    }
    
    /**
     * Execute a sequence of queries loaded from a file
     * This is useful for replaying query sequences for testing and benchmarking
     * 
     * @param filePath Path to the file containing saved queries
     * @return A list of query execution results (can be customized based on needs)
     */
    public List<Object> executeQuerySequenceFromFile(String filePath) {
        List<TypedQuery> queries = generateQuerySequenceFromFile(filePath);
        List<Object> results = new ArrayList<>();
        
        // Example of how to process and execute queries
        // This can be customized based on how queries should be executed
        for (TypedQuery typedQuery : queries) {
            Query query = typedQuery.getQuery();
            UserOpType opType = typedQuery.getUserOpType();
    
            LOG.debug("Executing query: {} of type {}", query, opType);
            
            // Placeholder for query execution result
            Object result = "Result for query " + query;
            results.add(result);
        }
        
        return results;
    }

    /**
     * Save the generated query sequence to a text file
     * @param queries The list of queries to save
     * @param filePath The path where to save the file
     */
    public void saveQueriesToFile(List<TypedQuery> queries, String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (TypedQuery typedQuery : queries) {
                Query q = typedQuery.getQuery();
                UserOpType userOpType = typedQuery.getUserOpType();
               
                // Write from,to,dateFrom,dateTomeasureId,viewportWidth,viewportHeight,queryType[,patternId]
                StringBuilder line = new StringBuilder();
                line.append(q.getFrom()).append(",")
                    .append(q.getTo()).append(",");

                // Write dateFrom, dateTo, 
                line.append(DateTimeUtil.format(q.getFrom())).append(",")
                    .append(DateTimeUtil.format(q.getTo())).append(",")
                    .append(q.getMeasures().get(0));
                
                // Add additional measures if they exist
                for (int i = 1; i < q.getMeasures().size(); i++) {
                    line.append("|").append(q.getMeasures().get(i));
                }
                
                // Add viewport information
                line.append(",")
                    .append(q.getViewPort().getWidth()).append(",")
                    .append(q.getViewPort().getHeight());

                // Add accuracy information
                line.append(",").append(q.getAccuracy());

                // Add query type information
                boolean isPattern = q instanceof PatternQuery;
                line.append(",").append(isPattern ? "pattern" : "visual");
                
                String s = "initial";
                 // Skip the initial query
                if(userOpType != null) {
                    switch (userOpType) {
                        case P:
                            s = "pan";
                            break;
                        case ZI:
                            s = "zoomIn";
                            break;
                        case ZO:
                            s = "zoomOut";
                            break;
                        case R:
                            s = "resize";
                            break;
                        case MC:
                            s = "measureChange";
                            break;
                        case PD:
                            s = PredefinedPattern.getPatternById(typedQuery.getPatternId()).getName();
                            break;
                        default:
                            break;
                    }
                    
                }
                line.append(",").append(s);
                if (isPattern) {
                    AggregateInterval timeUnit = ((PatternQuery) q).getTimeUnit();
                    line.append(",").append(String.valueOf(timeUnit.getMultiplier()) + " " + timeUnit.getChronoUnit());
                }
                writer.write(line.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            LOG.error("Error saving queries to file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
