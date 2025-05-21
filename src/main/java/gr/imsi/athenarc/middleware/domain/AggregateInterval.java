package gr.imsi.athenarc.middleware.domain;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Represents an aggregation time interval.
 */
public class AggregateInterval implements Comparable<AggregateInterval> {
    /**
     * Default interval used when no specific interval is specified.
     */
    public static final AggregateInterval DEFAULT = of(1, ChronoUnit.MILLIS);

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

    /**
     * Returns true if this interval is larger than the other interval.
     * 
     * @param other The other interval to compare with
     * @return true if this interval is larger
     */
    public boolean isLargerThan(AggregateInterval other) {
        return this.toDuration().toMillis() > other.toDuration().toMillis();
    }
    
    /**
     * Returns true if this interval is smaller than the other interval.
     * 
     * @param other The other interval to compare with
     * @return true if this interval is smaller
     */
    public boolean isSmallerThan(AggregateInterval other) {
        return this.toDuration().toMillis() < other.toDuration().toMillis();
    }

}