package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.util.Collections;
import java.util.List;

public class SingleNode implements PatternNode {
    
    private final SegmentSpecification spec;
    private final RepetitionFactor repetitionFactor;

    public SingleNode(SegmentSpecification spec,
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