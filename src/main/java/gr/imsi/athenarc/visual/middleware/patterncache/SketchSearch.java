package gr.imsi.athenarc.visual.middleware.patterncache;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;


public class SketchSearch {

    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);
    private final List<Sketch> sketches;
    private final PatternQuery query;

    // Pointers to track progress in sketches and segments
    private int currentSketchIndex;
    private int currentSegmentIndex;

    // Keep track of how many sketches long the current segment is.
    private int segmentMatchCount;

    // Composite sketch accumulated over the sketches in the segment.
    private Sketch compositeSketch;

    public SketchSearch(List<Sketch> sketches, PatternQuery query) {
        this.sketches = sketches;
        this.query = query;
        this.currentSketchIndex = 0;
        this.currentSegmentIndex = 0;
        this.segmentMatchCount = 0;
        this.compositeSketch = null;
    }

    /**
     * Runs the automaton over the sketches list.
     * Returns true if all segments in the PatternQuery are matched.
     */
    public boolean run() {
        // Get the list of segment specifications from the query.
        List<SegmentSpecification> segments = query.getSegmentSpecifications();

        while (currentSketchIndex < sketches.size()) {
            SegmentSpecification currentSegment = segments.get(currentSegmentIndex);

            while(segmentMatchCount < currentSegment.getTimeFilter().getTimeLow() && 
                    !(segmentMatchCount >= currentSegment.getTimeFilter().getTimeHigh())){
                if(segmentMatchCount == 0){
                    Sketch currentSketch = sketches.get(currentSketchIndex);
                    compositeSketch = new Sketch(currentSketch.getFrom(), currentSketch.getTo());
                    compositeSketch.addAggregatedDataPoint(currentSketch);
                }
                else{
                    if(currentSketchIndex + segmentMatchCount >= sketches.size()){
                        break;
                    }
                    Sketch nextSketch = sketches.get(currentSketchIndex + segmentMatchCount);
                    LOG.debug("Combining sketches {} and {}", currentSketchIndex, currentSketchIndex + segmentMatchCount);
                    compositeSketch.combine(nextSketch);
                    LOG.debug("Combined sketch: {}", compositeSketch);
                }
                segmentMatchCount += 1;
            }
            
            if(matchesComposite(compositeSketch, currentSegment.getValueFilter())){
                LOG.info("Segment match count: {}", segmentMatchCount);
                if(!transitionToNextSegment()){
                    break;
                };
            }
            else {
                transitionToNextSketch();
            }
        }
        return true;
    }

    /**
     * Moves to the next sketch and resets the segment match count.
     */
    private void transitionToNextSketch() {
        currentSketchIndex++;
        segmentMatchCount = 0;
    }

    /**
     * Resets counters and moves to the next segment.
     */
    private boolean transitionToNextSegment() {
        if(currentSegmentIndex == query.getSegmentSpecifications().size() - 1){
            currentSegmentIndex = 0;
            segmentMatchCount = 0;
            return false;
        }
        else {
            currentSegmentIndex ++;
            currentSketchIndex += segmentMatchCount;
            segmentMatchCount = 0;
            return true;
        }
    }

    /**
     * Checks whether the composite sketch satisfies the ValueFilter.
     * Uses the composite sketch's value to check against the filter.
     */
    private boolean matchesComposite(Sketch sketch, ValueFilter filter) {
        if (filter.isValueAny()) {
            return true;
        }
        double value = sketch.getValue();
        boolean match = value >= filter.getValueLow() && value <= filter.getValueHigh();
        LOG.debug("Match was {}, with value {} and filter {},{}", match, value, filter.getValueLow(), filter.getValueHigh());

        return match;
    }
}