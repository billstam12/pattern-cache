package gr.imsi.athenarc.visual.middleware.patterncache.query;

import java.util.List;

import gr.imsi.athenarc.visual.middleware.patterncache.query.repetition.RepetitionFactor;

/**
 * The root interface/type of a piece of pattern structure.
 * A pattern node can be:
 *   - A single "leaf" node referencing one SegmentSpecification.
 *   - A "group" node containing multiple children, treated as 
 *     a subpattern, all repeated together.
 */
public interface PatternNode {
    RepetitionFactor getRepetitionFactor();
    List<PatternNode> getChildren(); // might be empty for leaf nodes

    /**
     * Typically we store additional references for "which segment spec"
     * if this is a leaf, or "list of child nodes" if it is a group.
     * 
     * For distance calculation, you might define a method
     * "double computeDistance(...)"
     * or you let an external matching engine do the logic.
     */
}
