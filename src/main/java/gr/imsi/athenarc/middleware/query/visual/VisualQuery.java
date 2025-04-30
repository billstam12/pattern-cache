package gr.imsi.athenarc.middleware.query.visual;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.query.QueryType;
import gr.imsi.athenarc.middleware.query.Query;

/**
 * Represents a query for visualizing time series data.
 */
public class VisualQuery implements TimeInterval, Query {

    private final long from;
    private final long to;
    private final List<Integer> measures;
    private final ViewPort viewPort;
    private final double accuracy;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure; // New field for per-measure intervals

    /**
     * Creates a new visual query.
     *
     * @param from The start timestamp
     * @param to The end timestamp
     * @param measures The list of measure IDs to visualize
     * @param width The viewport width
     * @param height The viewport height
     * @param accuracy The accuracy level (0.0-1.0)
     */
    public VisualQuery(long from, long to, List<Integer> measures, int width, int height, double accuracy) {
        this(from, to, measures, width, height, accuracy, null);
    }
    

    /**
     * Creates a new visual query with measure-specific aggregation intervals.
     *
     * @param from The start timestamp
     * @param to The end timestamp
     * @param measures The list of measure IDs to visualize
     * @param width The viewport width
     * @param height The viewport height
     * @param accuracy The accuracy level (0.0-1.0)
     * @param measureAggregateIntervals Optional map of measure-specific aggregation intervals
     */
    public VisualQuery(long from, long to, List<Integer> measures, int width, int height, double accuracy,  Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.accuracy = accuracy;
        this.viewPort = new ViewPort(width, height);
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
    }

    /**
     * Creates a new builder for constructing VisualQuery objects.
     *
     * @return A new VisualQueryBuilder instance
     */
    public static VisualQueryBuilder builder() {
        return new VisualQueryBuilder();
    }

    /**
     * Gets the measure-specific aggregation intervals, if any.
     * 
     * @return Map of measure IDs to aggregation intervals, or null if not specified
     */
    public Map<Integer, AggregateInterval> getAggregateIntervalsPerMeasure() {
        return aggregateIntervalsPerMeasure;
    }
    
    /**
     * Checks if this query has measure-specific aggregation intervals.
     * 
     * @return true if measure-specific intervals are specified
     */
    public boolean hasAggregateIntervalsPerMeasure() {
        return aggregateIntervalsPerMeasure != null && !aggregateIntervalsPerMeasure.isEmpty();
    }

    @Override
    public QueryType getType() {
        return QueryType.VISUALIZATION;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public double getAccuracy(){
        return accuracy;
    }

    @Override
    public String toString() {
        return "VisualQuery{" +
                "from=" + from +
                ", to=" + to +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                ", accuracy=" + accuracy +
                (aggregateIntervalsPerMeasure != null ? ", measureIntervals=" + aggregateIntervalsPerMeasure : "") +
                '}';
    }
}
