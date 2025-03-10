package gr.imsi.athenarc.visual.middleware.patterncache.query;

import gr.imsi.athenarc.visual.middleware.patterncache.query.repetition.Exactly;
import gr.imsi.athenarc.visual.middleware.patterncache.query.repetition.RepetitionFactor;

public class SegmentSpecification {

  
    private final RepetitionFactor repetitionFactor;
    private final TimeFilter timeFilter;
    private ValueFilter valueFilter;
         
    public SegmentSpecification(TimeFilter timeFilter, ValueFilter valueFilter) {
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.repetitionFactor = new Exactly(1);
    }

    public SegmentSpecification(TimeFilter timeFilter, ValueFilter valueFilter, RepetitionFactor repetitionFactor) {
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.repetitionFactor = repetitionFactor;
    }
}