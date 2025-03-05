package gr.imsi.athenarc.visual.middleware.cache.query;

import gr.imsi.athenarc.visual.middleware.cache.query.repetition.RepetitionFactor;

public class SegmentSpecification {

    // -- Time constraints --
    private final boolean timeAny;    // if true, ignore time constraints
    private final double timeLow;     // if timeAny == false, min allowable length
    private final double timeHigh;    // if timeAny == false, max allowable length

    // -- Slope constraints --
    private final boolean slopeAny;   // if true, ignore slope constraints
    private final double slopeLow;
    private final double slopeHigh;

    private final RepetitionFactor repetitionFactor;
    
    public SegmentSpecification(
        boolean timeAny,
        double timeLow,
        double timeHigh,
        boolean slopeAny,
        double slopeLow,
        double slopeHigh,
        RepetitionFactor repetitionFactor
    ) {
        this.timeAny = timeAny;
        this.timeLow = timeLow;
        this.timeHigh = timeHigh;
        this.slopeAny = slopeAny;
        this.slopeLow = slopeLow;
        this.slopeHigh = slopeHigh;
        this.repetitionFactor = repetitionFactor;
    }
}