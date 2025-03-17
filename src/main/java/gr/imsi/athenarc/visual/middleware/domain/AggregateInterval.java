package gr.imsi.athenarc.visual.middleware.domain;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class AggregateInterval implements Comparable<AggregateInterval> {
    private int multiplier;
    private ChronoUnit chronoUnit;

    public AggregateInterval(int multiplier, ChronoUnit chronoUnit) {
        this.multiplier = multiplier;
        this.chronoUnit = chronoUnit;
    }

    public int getMultiplier() {
        return multiplier;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    public Duration toDuration() {
        return Duration.of(multiplier, chronoUnit);
    }

    @Override
    public String toString() {
        return "AggregateInterval{" +
                multiplier +
                " " +
                chronoUnit +
                '}';
    }

    @Override
    public int compareTo(AggregateInterval o) {
        return Long.compare(this.toDuration().toMillis(), o.toDuration().toMillis());
    }

}