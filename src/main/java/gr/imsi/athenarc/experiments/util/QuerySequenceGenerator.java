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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
    private float minShift = 0.1f;
    private float maxShift = 0.5f;

    private float zoomFactor = 2.0f;
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
            stateTransitionMatrix.put(fromState, new HashMap<>());
        }
    }
    
    /**
     * Set transition probability from one state to another
     * @param fromState The starting state
     * @param toState The destination state
     * @param probability The transition probability
     */
    public void setStateTransitionProbability(UserOpType fromState, UserOpType toState, double probability) {
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
        
        // Enable state transitions if any were found
        if (useStateTransitions) {
            LOG.info("Configured state transitions from properties");
        }
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
        
        // Calculate cumulative probabilities
        double total = transitions.values().stream().mapToDouble(Double::doubleValue).sum();
        if (total <= 0) {
            // Fall back to random if probabilities don't sum up
            return randomStateFromProbabilities();
        }
        
        // Select a state using the transition probabilities
        double rand = mainRandom.nextDouble() * total;
        double cumulative = 0;
        
        for (Map.Entry<UserOpType, Double> entry : transitions.entrySet()) {
            cumulative += entry.getValue();
            if (rand <= cumulative) {
                return entry.getKey();
            }
        }
        
        // Fallback to last state if something went wrong
        return transitions.keySet().iterator().next();
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
                            PatternQuery patternQuery = (PatternQuery) q;
                            s = PredefinedPattern.getPatternById(patternQuery.getPatternId()).getName();
                            break;
                        default:
                            break;
                    }
                    
                    line.append(",").append(s);
                }
                writer.write(line.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            LOG.error("Error saving queries to file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public List<TypedQuery> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
        Random shiftRandom = new Random(mainRandom.nextLong());
        double[] shifts = shiftRandom.doubles(count, minShift, maxShift).toArray();
        
        Random zoomRandom = new Random(mainRandom.nextLong());
        double[] zooms = zoomRandom.doubles(count, 1, zoomFactor).toArray();
        
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
        queries.add(new TypedQuery(q0, null)); //op type null = initial query
        Query q = q0;
        
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
            
            zoomFactor = (float) zooms[i];
            TimeRange timeRange = null;
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
            else if (zoomFactor > 1 && opType.equals(ZI)) {
                timeRange = zoomIn(q);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                timeRange = zoomOut(q);
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

            // Time range handling
            if (timeRange == null) timeRange = new TimeRange(q.getFrom(), q.getTo());
            else if ((timeRange.getFrom() == q.getFrom() && timeRange.getTo() == q.getTo())) {
                opType = ZI;
                timeRange = zoomIn(q);
            }
            
            // pattern
            if(opType.equals(PD)){
                Random patternRandom = new Random(mainRandom.nextInt());
                q = generatePatternQuery(q.getFrom(), q.getTo(), q.getMeasures().get(0), q.getViewPort(), patternRandom);
            } // else create visual query
            else {
                // Generate a visual query
                q = new VisualQueryBuilder()
                    .withTimeRange(timeRange.getFrom(), timeRange.getTo())
                    .withMeasures(measures)
                    .withViewPort(viewPort.getWidth(), viewPort.getHeight())
                    .withAccuracy(q0.getAccuracy())
                    .build();
            }
            queries.add(new TypedQuery(q, opType));
            
            // Update current state if using state transitions
            if (useStateTransitions) {
                currentState = opType;
            }
        }
        return queries;
    }
    
    /**
     * Generates a pattern query with randomized pattern parameters
     */
    private PatternQuery generatePatternQuery(long from, long to, int measure, ViewPort viewPort, Random random) {
        // Generate an aggregation interval two to four times smaller than the time range
        int level = random.nextInt(3) + 2; // Generates 2, 3, or 4
        AggregateInterval timeUnit = DateTimeUtil.roundDownToCalendarBasedInterval((to - from) / level * viewPort.getWidth());

        // Generate a random aggregation type
        AggregationType[] types = AggregationType.values();
        AggregationType aggregationType = types[random.nextInt(types.length)];
        
        // Choose a predefined pattern ID and get the pattern
        List<Integer> patternIds = PredefinedPattern.getAllPatternIds();
        int patternId = patternIds.get(random.nextInt(patternIds.size()));
        PredefinedPattern predefinedPattern = PredefinedPattern.getPatternById(patternId);
        
        // Create and return a pattern query with the selected pattern ID
        return new PatternQuery(
            from, to, measure, timeUnit, aggregationType, 
            predefinedPattern.getPatternNodes(), viewPort, 1.0, patternId
        );
    }
    
    private TimeRange pan(Query query, double shift, Direction direction) {
        long from = query.getFrom();
        long to = query.getTo();
        long timeShift = (long) ((to - from) * shift);

        switch (direction) {
            case L:
                if(dataset.getTimeRange().getFrom() >= (from - timeShift)){
                    opType = ZI;
                    return zoomIn(query);
                }
                to = to - timeShift;
                from = from - timeShift;
                break;
            case R:
                if(dataset.getTimeRange().getTo() <= (to + timeShift)){
                    opType = ZI;
                    return zoomIn(query);
                }
                to = to + timeShift;
                from = from + timeShift;
                break;
            default:
                return new TimeRange(from, to);

        }
        return new TimeRange(from, to);
    }


    private TimeRange zoomIn(Query query) {
        return zoom(query, 1f / zoomFactor);
    }

    private TimeRange zoomOut(Query query) {
        return zoom(query, zoomFactor);
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
            newTo = dataset.getTimeRange().getTo();
            newFrom = dataset.getTimeRange().getFrom();
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

        // Look for state transition flag
        String useTransitions = properties.getProperty("useStateTransitions");
        if (useTransitions != null) {
            this.useStateTransitions = Boolean.parseBoolean(useTransitions);
        }
        
        // If state transitions are enabled, also load the transition matrix
        if (this.useStateTransitions) {
            setStateTransitionsFromProperties(properties);
        }
        
        LOG.info("Set probabilities from config: pattern={}, pan={}, zoomIn={}, zoomOut={}, resize={}, measureChange={}, useStateTransitions={}", 
            patternDetectionProbability, panProbability, zoomInProbability, zoomOutProbability, resizeProbability, measureChangeProbability, useStateTransitions);
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
}
