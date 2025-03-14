package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.util.List;

import gr.imsi.athenarc.visual.middleware.patterncache.query.repetition.RepetitionFactor;

public class GroupNode implements PatternNode {
    private final List<PatternNode> children;
    private final RepetitionFactor repetitionFactor;

    public GroupNode(List<PatternNode> children,
                     RepetitionFactor repetitionFactor) {
        this.children = children;
        this.repetitionFactor = repetitionFactor;
    }

    @Override
    public RepetitionFactor getRepetitionFactor() {
        return repetitionFactor;
    }

    @Override
    public List<PatternNode> getChildren() {
        return children;
    }
}