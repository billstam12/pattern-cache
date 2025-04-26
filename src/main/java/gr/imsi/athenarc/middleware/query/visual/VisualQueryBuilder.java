package gr.imsi.athenarc.middleware.query.visual;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder class for creating VisualQuery objects using a fluent interface.
 * This makes it easier to construct complex visual queries with many parameters.
 */
public class VisualQueryBuilder {
    private long from;
    private long to;
    private List<Integer> measures = new ArrayList<>();
    private int width = 800; // Default width
    private int height = 400; // Default height
    private double accuracy = 1.0; // Default accuracy
    private AbstractDataset dataset;
    private Map<Integer, AggregateInterval> measureAggregateIntervals;

    /**
     * Start building a new VisualQuery.
     */
    public VisualQueryBuilder() {
        // Initialize with defaults
    }

    /**
     * Set the starting timestamp for the query.
     * 
     * @param from The starting timestamp in milliseconds since epoch
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withTimeFrom(long from) {
        this.from = from;
        return this;
    }

    /**
     * Set the ending timestamp for the query.
     * 
     * @param to The ending timestamp in milliseconds since epoch
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withTimeTo(long to) {
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
    public VisualQueryBuilder withTimeRange(long from, long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    /**
     * Set the dataset for the query. Some initialization can be done based on the dataset.
     * 
     * @param dataset The dataset to query
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withDataset(AbstractDataset dataset) {
        this.dataset = dataset;
        return this;
    }

    /**
     * Add a single measure to the query.
     * 
     * @param measureId The ID of the measure to query
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withMeasure(int measureId) {
        this.measures.add(measureId);
        return this;
    }

    /**
     * Set multiple measures for the query.
     * 
     * @param measureIds The list of measure IDs to query
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withMeasures(List<Integer> measureIds) {
        this.measures.clear();
        this.measures.addAll(measureIds);
        return this;
    }

    /**
     * Set multiple measures for the query using varargs.
     * 
     * @param measureIds The array of measure IDs to query
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withMeasures(Integer... measureIds) {
        this.measures.clear();
        this.measures.addAll(Arrays.asList(measureIds));
        return this;
    }

    /**
     * Set the width and height of the viewport.
     * 
     * @param width The width of the viewport in pixels
     * @param height The height of the viewport in pixels
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withViewPort(int width, int height) {
        this.width = width;
        this.height = height;
        return this;
    }

    /**
     * Set the desired accuracy level for the query result.
     * 
     * @param accuracy The accuracy level between 0.0 and 1.0
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withAccuracy(double accuracy) {
        if (accuracy < 0.0 || accuracy > 1.0) {
            throw new IllegalArgumentException("Accuracy must be between 0.0 and 1.0");
        }
        this.accuracy = accuracy;
        return this;
    }

    /**
     * Set measure-specific aggregation intervals.
     * This is useful for cache initialization or other scenarios where
     * different measures need different aggregation levels.
     * 
     * @param measureAggregateIntervals Map of measure IDs to their aggregation intervals
     * @return This builder for method chaining
     */
    public VisualQueryBuilder withMeasureAggregateIntervals(Map<Integer, AggregateInterval> measureAggregateIntervals) {
        this.measureAggregateIntervals = new HashMap<>(measureAggregateIntervals);
        return this;
    }

    /**
     * Build the VisualQuery object with the configured parameters.
     * 
     * @return A new VisualQuery instance
     * @throws IllegalStateException if required parameters are missing
     */
    public VisualQuery build() {
        validateState();
        
        // If we're using a dataset and time range is not set, use dataset's time range
        if (dataset != null) {
            if (from == 0) {
                from = dataset.getTimeRange().getFrom();
            }
            if (to == 0) {
                to = dataset.getTimeRange().getTo();
            }
        }

        return new VisualQuery(
            from, 
            to, 
            Collections.unmodifiableList(measures), 
            width, 
            height, 
            accuracy,
            measureAggregateIntervals
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
        if (from == 0 && dataset == null) {
            missingParams.add("from timestamp");
        }
        if (to == 0 && dataset == null) {
            missingParams.add("to timestamp");
        }
        if (measures.isEmpty()) {
            missingParams.add("at least one measure");
        }
        
        if (!missingParams.isEmpty()) {
            throw new IllegalStateException("Cannot build VisualQuery: missing " + String.join(", ", missingParams));
        }
        
        // Validate time range
        if (from >= to) {
            throw new IllegalStateException("Invalid time range: 'from' must be less than 'to'");
        }
    }
}
