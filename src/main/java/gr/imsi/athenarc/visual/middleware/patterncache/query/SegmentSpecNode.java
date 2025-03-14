package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.util.Collections;
import java.util.List;

import gr.imsi.athenarc.visual.middleware.patterncache.query.repetition.RepetitionFactor;

public class SegmentSpecNode implements PatternNode {
    
    private final SegmentSpecification spec;
    private final RepetitionFactor repetitionFactor;

    public SegmentSpecNode(SegmentSpecification spec,
                           RepetitionFactor repetitionFactor) {
        this.spec = spec;
        this.repetitionFactor = repetitionFactor;
    }

    @Override
    public RepetitionFactor getRepetitionFactor() {
        return repetitionFactor;
    }

    @Override
    public List<PatternNode> getChildren() {
        // A leaf node: no children
        return Collections.emptyList();
    }

    public SegmentSpecification getSpec() {
        return spec;
    }
}