package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PatternQuery {
    private long from;
    private long to;
    private int measure;
    private ChronoUnit timeUnit;
    private final List<SegmentSpecification> segmentSpecifications;

    public PatternQuery(long from, long to, int measure, ChronoUnit timeUnit, List<SegmentSpecification> segmentSpecifications) {
        this.from = from;
        this.to = to;
        this.measure = measure;
        this.timeUnit = timeUnit;
        this.segmentSpecifications = segmentSpecifications;
    }

    public List<SegmentSpecification> getSegmentSpecifications() {
        return segmentSpecifications;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }   

    public int getMeasure() {
        return measure;
    }

    public ChronoUnit getChronoUnit() {
        return timeUnit;
    }

}
