package gr.imsi.athenarc.middleware.domain;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class AggregateInterval implements Comparable<AggregateInterval> {
    private long multiplier;
    private ChronoUnit chronoUnit;

    private AggregateInterval(long multiplier, ChronoUnit chronoUnit) {
        this.multiplier = multiplier;
        this.chronoUnit = chronoUnit;
    }

    public long getMultiplier() {
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


    public static AggregateInterval of(long multiplier, ChronoUnit chronoUnit) {
        return new AggregateInterval(multiplier, chronoUnit);
    }

    public static AggregateInterval fromMillis(long milliseconds) {
        return new AggregateInterval(milliseconds, ChronoUnit.MILLIS);
    }

}