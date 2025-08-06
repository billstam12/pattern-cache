package gr.imsi.athenarc.middleware.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DateTimeUtil {

    public static final ZoneId UTC = ZoneId.of("UTC");
    public final static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";
    public final static DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_FORMAT);
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtil.class);

    // Pre-defined calendar intervals for rounding operations
    public static final List<AggregateInterval> CALENDAR_INTERVALS = initializeCalendarIntervals();

    // Initialize the calendar intervals
    private static List<AggregateInterval> initializeCalendarIntervals() {
        List<AggregateInterval> intervals = new ArrayList<>();

        // Milliseconds
        for (int i = 1; i <= 500; i ++){
            if (1000 % i == 0) {
                intervals.add(AggregateInterval.of(i, ChronoUnit.MILLIS));
            }
        }
        // Seconds
        for (int i = 1; i <= 30; i ++){
            if (60 % i == 0) {
                intervals.add(AggregateInterval.of(i, ChronoUnit.SECONDS));
            }
        }
        
        // Minutes
        for (int i = 1; i <= 30; i ++){
            if (60 % i == 0) {
                intervals.add(AggregateInterval.of(i, ChronoUnit.MINUTES));
            }
        }

        // Hours
        for (int i = 1; i <= 12; i ++){
            if (24 % i == 0) {
                intervals.add(AggregateInterval.of(i, ChronoUnit.HOURS));
            }
        }       
         
        // Days 
        intervals.add(AggregateInterval.of(1, ChronoUnit.DAYS));
        
        return intervals;
    }

     public static long parseDateTimeString(String s, String timeFormat) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        return parseDateTimeStringInternal(s, formatter, UTC);
    }

    public static long parseDateTimeString(String s, DateTimeFormatter formatter) {
        return parseDateTimeStringInternal(s, formatter, UTC);
    }

    public static long parseDateTimeString(String s, DateTimeFormatter formatter, ZoneId zoneId) {
        return parseDateTimeStringInternal(s, formatter, zoneId);
    }

    public static long parseDateTimeString(String s) {
        return parseDateTimeStringInternal(s, DEFAULT_FORMATTER, UTC);
    }

    private static long parseDateTimeStringInternal(String s, DateTimeFormatter formatter, ZoneId zoneId) {
        try {
            // Try parsing as LocalDateTime
            return LocalDateTime.parse(s, formatter).atZone(zoneId).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // If parsing as LocalDateTime fails, try parsing as LocalDate
            return LocalDate.parse(s, formatter).atStartOfDay(zoneId).toInstant().toEpochMilli();
        }
    }

    public static String format(final long timeStamp) {
        return formatTimeStamp( timeStamp, DEFAULT_FORMATTER);
    }

    public static String format(final long timeStamp, final ZoneId zone) {
        return format(timeStamp, DEFAULT_FORMATTER,  zone);
    }

    public static String format(final long timeStamp, final String format) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return Instant.ofEpochMilli(timeStamp)
                .atZone(UTC)
                .format(formatter);
    }

    public static String format(final long timeStamp, final String format, final ZoneId zone) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String format( final long timeStamp, final DateTimeFormatter formatter, final ZoneId zone) {
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String formatTimeStamp(final long timeStamp) {
        return formatTimeStamp(timeStamp, DEFAULT_FORMATTER);
    }

    public static String formatTimeStamp(final long timeStamp, final String format) {
        return format(timeStamp, format, UTC);
    }

    public static String formatTimeStamp(final long timeStamp, final DateTimeFormatter formatter ) {
        return format(timeStamp, formatter, UTC);
    }
 
    public static int numberOfIntervals(final long startTime, final long endTime, AggregateInterval aggregateInterval) {
        return (int) Math.ceil((double)(endTime - startTime) / aggregateInterval.toDuration().toMillis());
    }

    public static int indexInInterval(final long startTime, final long endTime, final AggregateInterval aggregateInterval, final long time) {
        return (int) ((time - startTime ) / aggregateInterval.toDuration().toMillis());
    }

    /**
     *
     * @param pixelColumnInterval interval of the pixel columns
     * @param ranges missing ranges to group
     * @return
     */
    public static List<TimeInterval> groupIntervals(AggregateInterval pixelColumnInterval, List<TimeInterval> ranges) {
        if(ranges.size() == 0) return ranges;
        List<TimeInterval> groupedRanges = new ArrayList<>();
        TimeInterval currentGroup = ranges.get(0);
        long pixelColumnIntervalMillis = pixelColumnInterval.toDuration().toMillis();
        for(TimeInterval currentRange : ranges){
            if (currentGroup.getTo() + (pixelColumnIntervalMillis * 10) >= currentRange.getFrom() && groupedRanges.size() > 0) {
                // Extend the current group
                currentGroup = new TimeRange(currentGroup.getFrom(), currentRange.getTo());
                groupedRanges.set(groupedRanges.size() - 1, currentGroup);
            } else {
                // Start a new group
                currentGroup = currentRange;
                groupedRanges.add(currentGroup);
            }
        }
        return groupedRanges;
    }

    /**
     * Rounds down a millisecond interval to the closest calendar-based interval.
     * Supports standard calendar units and their common multiples (e.g., 5min, 15min, 30min).
     * Common multiples are those that exactly map to the next calendar unit.
     *
     * @param intervalMs interval in milliseconds
     * @return AggregateInterval rounded to the closest calendar-based interval
     */
    public static AggregateInterval roundDownToCalendarBasedInterval(long intervalMs) { 
        // Find closest calendar interval
        AggregateInterval result = CALENDAR_INTERVALS.get(0);  // Default to smallest interval
        
        for (AggregateInterval interval : CALENDAR_INTERVALS) {
            long intervalDuration = interval.toDuration().toMillis();
            if (intervalDuration <= intervalMs) {
                result = interval;
            } else {
                // Stop once we find an interval that exceeds intervalMs
                break;
            }
        }
        
        return AggregateInterval.of(result.getMultiplier(), result.getChronoUnit());
    }

    public static AggregateInterval roundUpToCalendarBasedInterval(long intervalMs) { 
    // Find closest calendar interval that is >= intervalMs
    for (AggregateInterval interval : CALENDAR_INTERVALS) {
        long intervalDuration = interval.toDuration().toMillis();
        if (intervalDuration >= intervalMs) {
            return AggregateInterval.of(interval.getMultiplier(), interval.getChronoUnit());
        }
    }
    
    // If no interval is large enough, return the largest available interval
    AggregateInterval largest = CALENDAR_INTERVALS.get(CALENDAR_INTERVALS.size() - 1);
    return AggregateInterval.of(largest.getMultiplier(), largest.getChronoUnit());
}
    
    /**
     * Determines if a smaller interval can be aggregated into a larger target interval.
     * This is more complex for calendar-based intervals due to varying durations.
     * 
     * @param smaller The smaller interval
     * @param target The target interval
     * @return true if smaller can be aggregated into target
     */
    public static boolean isCompatibleWithTarget(AggregateInterval smaller, AggregateInterval target) {
        ChronoUnit smallerUnit = smaller.getChronoUnit();
        ChronoUnit targetUnit = target.getChronoUnit();
        long smallerMultiplier = smaller.getMultiplier();
        long targetMultiplier = target.getMultiplier();
        
        // Same unit type - check if target multiplier is divisible by smaller multiplier
        if (smallerUnit == targetUnit) {
            return targetMultiplier % smallerMultiplier == 0;
        }
        
        // Different units - check compatibility based on hierarchy
        switch (targetUnit) {
            case MILLIS:
                // Nothing is smaller than milliseconds in our system
                return false;
                
            case SECONDS:
                // Check if milliseconds can aggregate into seconds
                if (smallerUnit == ChronoUnit.MILLIS) {
                    return (1000 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                return false;
                
            case MINUTES:
                // Check if seconds or milliseconds can aggregate into minutes
                if (smallerUnit == ChronoUnit.SECONDS) {
                    return (60 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.MILLIS) {
                    return (60000 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                return false;
                       
            case HOURS:
                // Check if minutes, seconds, or milliseconds can aggregate into hours
                if (smallerUnit == ChronoUnit.MINUTES) {
                    return (60 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.SECONDS) {
                    return (3600 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.MILLIS) {
                    return (3600000 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                return false;
                
            case DAYS:
                // Check if hours, minutes, seconds, or milliseconds can aggregate into days
                if (smallerUnit == ChronoUnit.HOURS) {
                    return (24 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.MINUTES) {
                    return (1440 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.SECONDS) {
                    return (86400 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.MILLIS) {
                    return (86400000 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                return false;   
            case WEEKS:
                // Check if days, hours, etc. can aggregate into weeks
                if (smallerUnit == ChronoUnit.DAYS) {
                    return (7 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.HOURS) {
                    return (168 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0); // 7*24 hours
                }
                if (smallerUnit == ChronoUnit.MINUTES) {
                    return (10080 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0); // 7*24*60 minutes
                }
                return false;
                
            case MONTHS:
                // Months have varying lengths, so we're more restrictive
                if (smallerUnit == ChronoUnit.DAYS) {
                    // For months, we only accept 1 day as compatible since month lengths vary
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                if (smallerUnit == ChronoUnit.WEEKS) {
                    // For months, we only accept 1 week as compatible since month lengths vary
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                return false;
            case YEARS:
                if (smallerUnit == ChronoUnit.MONTHS) {
                    return (12 % smallerMultiplier == 0) && (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.DAYS) {
                    // For years, we only accept 1 day as compatible due to leap years
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                if (smallerUnit == ChronoUnit.WEEKS) {
                    // For years, we only accept 1 week as compatible due to leap years
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                return false;   
            default:
                return false;
        }
    }

    /**
     * Aligns a timestamp to the nearest time unit boundary.
     * 
     * @param timestamp The timestamp to align
     * @param timeUnit The time unit to align to
     * @param floor If true, align to floor (start of unit), otherwise ceiling (end of unit)
     * @return The aligned timestamp
     */
    public static TimeInterval alignIntervalToTimeUnitBoundary(TimeInterval interval, AggregateInterval timeUnit) {
        long from = interval.getFrom();
        long to = interval.getTo();
        long alignFrom = alignToTimeUnitBoundary(from, timeUnit, true);
        long alignTo = alignToTimeUnitBoundary(to, timeUnit, false);
        return new TimeRange(alignFrom, alignTo);
    }

    /**
     * Aligns a timestamp to the nearest time unit boundary.
     * 
     * @param timestamp The timestamp to align
     * @param timeUnit The time unit to align to
     * @param floor If true, align to floor (start of unit), otherwise ceiling (end of unit)
     * @return The aligned timestamp
     */
    public static long alignToTimeUnitBoundary(long timestamp, AggregateInterval timeUnit, boolean floor) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        
        // For chronological units like DAYS, HOURS, etc., use Java's truncatedTo
        ChronoUnit chronoUnit = timeUnit.getChronoUnit();
        long multiplier = timeUnit.getMultiplier();
        
        if (multiplier == 1) {
            // Simple case - just truncate to the unit boundary
            if (floor) {
                return instant.atZone(UTC).truncatedTo(chronoUnit).toInstant().toEpochMilli();
            } else {
                // For ceiling, go to next unit and subtract 1ms
                return instant.atZone(UTC)
                        .truncatedTo(chronoUnit)
                        .plus(1, chronoUnit)
                        .toInstant().toEpochMilli();
            }
        } else {
            switch (chronoUnit) {
                case MILLIS:
                    // For milliseconds, just multiply by the multiplier
                    return alignToMultipleOf(timestamp, 1, multiplier, floor);
                case SECONDS:
                    return alignToMultipleOf(timestamp, 1000, multiplier, floor);
                case MINUTES:
                    return alignToMultipleOf(timestamp, 60 * 1000, multiplier, floor);
                case HOURS:
                    return alignToMultipleOf(timestamp, 3600 * 1000, multiplier, floor);
                case DAYS:
                    return alignToMultipleOf(timestamp, 24 * 3600 * 1000, multiplier, floor);
                // Add more cases as needed
                default:
                    LOG.warn("Unsupported chrono unit for alignment: {}", chronoUnit);
                    return timestamp;
            }
        }
    }

    /**
     * Aligns a timestamp to a multiple of a base unit.
     * 
     * @param timestamp The timestamp to align
     * @param baseUnitMs The base unit in milliseconds (e.g., 60*1000 for minutes)
     * @param multiplier The multiplier (e.g., 15 for 15 minutes)
     * @param floor If true, round down, otherwise round up
     * @return The aligned timestamp
     */
    private static long alignToMultipleOf(long timestamp, long baseUnitMs, long multiplier, boolean floor) {
        // Get the epoch second of the day
        long msOfDay = timestamp % (24 * 3600 * 1000);
        long dayStart = timestamp - msOfDay;
        
        // Calculate how many complete units fit
        long unitsElapsed = msOfDay / (baseUnitMs * multiplier);
        
        if (floor) {
            // For floor, just multiply by complete units
            return dayStart + (unitsElapsed * baseUnitMs * multiplier);
        } else {
            // For ceiling, add one more unit if there's a remainder
            if (msOfDay % (baseUnitMs * multiplier) > 0) {
                unitsElapsed++;
            }
            return dayStart + (unitsElapsed * baseUnitMs * multiplier);
        }
    }

    public static Map<Integer, List<TimeInterval>> alignIntervalsToTimeUnitBoundary(Map<Integer, List<TimeInterval>> intervals, Map<Integer, AggregateInterval> aggregateIntervals) {
        // Create a new map to store aligned intervals
        Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure = new HashMap<>();
        
        // For each measure and its intervals, create aligned copies and store them in the new map
        for (Map.Entry<Integer, List<TimeInterval>> entry : intervals.entrySet()) {
            int measure = entry.getKey();
            List<TimeInterval> originalIntervals = entry.getValue();
            List<TimeInterval> alignedIntervals = new ArrayList<>();
            
            for (TimeInterval interval : originalIntervals) {
                TimeInterval alignedInterval = DateTimeUtil.alignIntervalToTimeUnitBoundary(interval, aggregateIntervals.get(measure));
                alignedIntervals.add(alignedInterval);
            }
            
            alignedIntervalsPerMeasure.put(measure, alignedIntervals);
        }
        LOG.info("Aligned missing intervals per measure: {}", alignedIntervalsPerMeasure);    
        return alignedIntervalsPerMeasure;
    }
}
