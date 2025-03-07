package gr.imsi.athenarc.visual.middleware.cache.query;

import java.util.List;

public class PatternQuery {
    private final List<SegmentSpecification> segmentSpecifications;

    public PatternQuery(List<SegmentSpecification> segmentSpecifications) {
        this.segmentSpecifications = segmentSpecifications;
    }

    public List<SegmentSpecification> getSegmentSpecifications() {
        return segmentSpecifications;
    }
}
