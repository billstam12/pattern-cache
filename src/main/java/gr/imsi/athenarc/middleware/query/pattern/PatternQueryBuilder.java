package gr.imsi.athenarc.middleware.query.pattern;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.ViewPort;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder class for creating PatternQuery objects using a fluent interface.
 * This makes it easier to construct complex pattern queries with many parameters.
 */
public class PatternQueryBuilder {
    private long from;
    private long to;
    private int measure;
    private AggregateInterval timeUnit;
    private AggregationType aggregationType = AggregationType.LAST_VALUE; // Default aggregation type
    private List<PatternNode> patternNodes = new ArrayList<>();
    private ViewPort viewPort = new ViewPort(800, 400); // Default viewport
    private double accuracy = 1.0; // Default accuracy

    /**
     * Start building a new PatternQuery.
     */
    public PatternQueryBuilder() {
        // Initialize with defaults
    }

    /**
     * Set the starting timestamp for the query.
     * 
     * @param from The starting timestamp in milliseconds since epoch
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withTimeFrom(long from) {
        this.from = from;
        return this;
    }

    /**
     * Set the ending timestamp for the query.
     * 
     * @param to The ending timestamp in milliseconds since epoch
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withTimeTo(long to) {
        this.to = to;
        return this;
    }

    /**
     * Set both the starting and ending timestamps in a single call.
     * 
     * @param from The starting timestamp in milliseconds since epoch
     * @param to The ending timestamp in milliseconds since epoch
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withTimeRange(long from, long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    /**
     * Set the measure for the pattern query.
     * 
     * @param measure The measure ID to use for pattern matching
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withMeasure(int measure) {
        this.measure = measure;
        return this;
    }

    /**
     * Set the time unit for aggregation.
     * 
     * @param timeUnit The aggregation time unit
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withTimeUnit(AggregateInterval timeUnit) {
        this.timeUnit = timeUnit;
        return this;
    }

    /**
     * Set the aggregation type.
     * 
     * @param aggregationType The type of aggregation to use
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
        return this;
    }

    /**
     * Set the pattern nodes (segments) for pattern matching.
     * 
     * @param patternNodes The list of pattern nodes to match
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withPatternNodes(List<PatternNode> patternNodes) {
        this.patternNodes = new ArrayList<>(patternNodes);
        return this;
    }

    /**
     * Set the width and height of the viewport.
     * 
     * @param width The width of the viewport in pixels
     * @param height The height of the viewport in pixels
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withViewPort(int width, int height) {
        this.viewPort = new ViewPort(width, height);
        return this;
    }

    /**
     * Set the viewport object directly.
     * 
     * @param viewPort The viewport object
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withViewPort(ViewPort viewPort) {
        this.viewPort = viewPort;
        return this;
    }

    /**
     * Set the desired accuracy level for the query result.
     * 
     * @param accuracy The accuracy level between 0.0 and 1.0
     * @return This builder for method chaining
     */
    public PatternQueryBuilder withAccuracy(double accuracy) {
        if (accuracy < 0.0 || accuracy > 1.0) {
            throw new IllegalArgumentException("Accuracy must be between 0.0 and 1.0");
        }
        this.accuracy = accuracy;
        return this;
    }

    /**
     * Build the PatternQuery object with the configured parameters.
     * 
     * @return A new PatternQuery instance
     * @throws IllegalStateException if required parameters are missing
     */
    public PatternQuery build() {
        validateState();
        
        return new PatternQuery(
            from, 
            to, 
            measure,
            timeUnit,
            aggregationType,
            patternNodes,
            viewPort,
            accuracy
        );
    }

    /**
     * Validate that the builder has all required parameters set.
     * 
     * @throws IllegalStateException if required parameters are missing
     */
    private void validateState() {
        List<String> missingParams = new ArrayList<>();
        
        // Check for required parameters
        if (from == 0) {
            missingParams.add("from timestamp");
        }
        if (to == 0) {
            missingParams.add("to timestamp");
        }
        if (timeUnit == null) {
            missingParams.add("time unit");
        }
        if (patternNodes == null || patternNodes.isEmpty()) {
            missingParams.add("pattern nodes");
        }
        
        if (!missingParams.isEmpty()) {
            throw new IllegalStateException("Cannot build PatternQuery: missing " + String.join(", ", missingParams));
        }
        
        // Validate time range
        if (from >= to) {
            throw new IllegalStateException("Invalid time range: 'from' must be less than 'to'");
        }
    }
}
