package gr.imsi.athenarc.visual.middleware.patterncache.query;

public class TimeFilter {
    // -- Time constraints --
    private final boolean timeAny;    // if true, ignore time constraints
    private final int timeLow;     // if timeAny == false, min allowable length
    private final int timeHigh;    // if timeAny == false, max allowable length

    public TimeFilter(boolean timeAny, int timeLow, int timeHigh) {
        this.timeAny = timeAny;
        this.timeLow = timeLow;
        this.timeHigh = timeHigh;
    }

    public boolean isTimeAny() {
        return timeAny;
    }

    public int getTimeLow() {
        return timeLow;
    }
    
    public int getTimeHigh() {
        return timeHigh;
    }
}
