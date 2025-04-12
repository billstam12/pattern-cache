package gr.imsi.athenarc.visual.middleware.domain;

import java.util.List;
import java.util.Objects;

/**
 * Represents aggregation request parameters specific to a measure
 */
public class MeasureAggregationRequest {
    private final int measureIndex;
    private final List<TimeInterval> missingIntervals;
    private final AggregateInterval aggregateInterval;

    public MeasureAggregationRequest(int measureIndex, List<TimeInterval> missingIntervals, AggregateInterval aggregateInterval) {
        this.measureIndex = measureIndex;
        this.missingIntervals = missingIntervals;
        this.aggregateInterval = aggregateInterval;
    }

    public int getMeasureIndex() {
        return measureIndex;
    }

    public List<TimeInterval> getMissingIntervals() {
        return missingIntervals;
    }

    public AggregateInterval getAggregateInterval() {
        return aggregateInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeasureAggregationRequest that = (MeasureAggregationRequest) o;
        return measureIndex == that.measureIndex &&
                Objects.equals(missingIntervals, that.missingIntervals) &&
                Objects.equals(aggregateInterval, that.aggregateInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(measureIndex, missingIntervals, aggregateInterval);
    }
}
