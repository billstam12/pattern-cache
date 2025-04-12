package gr.imsi.athenarc.visual.middleware.query.pattern;

import java.util.List;

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
}
