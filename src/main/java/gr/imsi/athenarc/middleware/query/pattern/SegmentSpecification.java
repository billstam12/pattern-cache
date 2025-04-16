package gr.imsi.athenarc.middleware.query.pattern;


public class SegmentSpecification {

    private final TimeFilter timeFilter;
    private final ValueFilter valueFilter;
         
    public SegmentSpecification(TimeFilter timeFilter, ValueFilter valueFilter) {
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public ValueFilter getValueFilter() {
        return valueFilter;
    }
    
    public String toString(){
        return "SegmentSpecification: " + timeFilter + " " + valueFilter;
    }
}