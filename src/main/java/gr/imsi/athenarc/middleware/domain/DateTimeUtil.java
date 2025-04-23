package gr.imsi.athenarc.middleware.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;


public class DateTimeUtil {

    public static final ZoneId UTC = ZoneId.of("UTC");
    public final static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";
    public final static DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_FORMAT);
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtil.class);


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
        // Define common calendar-based intervals
        List<AggregateInterval> calendarIntervals = new ArrayList<>();
        
        // Seconds
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.SECONDS));
        calendarIntervals.add(new AggregateInterval(5, ChronoUnit.SECONDS));
        calendarIntervals.add(new AggregateInterval(10, ChronoUnit.SECONDS));
        calendarIntervals.add(new AggregateInterval(15, ChronoUnit.SECONDS));
        calendarIntervals.add(new AggregateInterval(30, ChronoUnit.SECONDS));
        
        // Minutes
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.MINUTES));
        calendarIntervals.add(new AggregateInterval(5, ChronoUnit.MINUTES));
        calendarIntervals.add(new AggregateInterval(10, ChronoUnit.MINUTES));
        calendarIntervals.add(new AggregateInterval(15, ChronoUnit.MINUTES));
        calendarIntervals.add(new AggregateInterval(20, ChronoUnit.MINUTES));
        calendarIntervals.add(new AggregateInterval(30, ChronoUnit.MINUTES));
        
        // Hours
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.HOURS));
        calendarIntervals.add(new AggregateInterval(2, ChronoUnit.HOURS));
        calendarIntervals.add(new AggregateInterval(3, ChronoUnit.HOURS));
        calendarIntervals.add(new AggregateInterval(4, ChronoUnit.HOURS));
        calendarIntervals.add(new AggregateInterval(6, ChronoUnit.HOURS));
        calendarIntervals.add(new AggregateInterval(12, ChronoUnit.HOURS));
        
        // Days and above
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.DAYS));
        // calendarIntervals.add(new AggregateInterval(1, ChronoUnit.WEEKS));
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.MONTHS));
        calendarIntervals.add(new AggregateInterval(3, ChronoUnit.MONTHS));
        calendarIntervals.add(new AggregateInterval(6, ChronoUnit.MONTHS));
        calendarIntervals.add(new AggregateInterval(1, ChronoUnit.YEARS));

        // Find closest calendar interval
        AggregateInterval result = calendarIntervals.get(0);  // Default to smallest interval
        
        for (AggregateInterval interval : calendarIntervals) {
            long intervalDuration = interval.toDuration().toMillis();
            if (intervalDuration <= intervalMs) {
                result = interval;
            } else {
                // Stop once we find an interval that exceeds intervalMs
                break;
            }
        }
        
        return new AggregateInterval(result.getMultiplier(), result.getChronoUnit());
    }

    /**
     * Finds all calendar-based intervals that can be aggregated to match the target interval.
     * For example, if the target interval is 1 day, then 1 hour intervals can be aggregated into days.
     *
     * @param targetInterval The target interval to match
     * @return List of AggregateIntervals that can be aggregated to the target interval
     */
    // public static List<AggregateInterval> findCompatibleIntervals(AggregateInterval targetInterval) {
    //     List<AggregateInterval> compatibleIntervals = new ArrayList<>();
        
    //     // Define common calendar-based intervals
    //     List<AggregateInterval> calendarIntervals = new ArrayList<>();
        
    //     // Seconds
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.SECONDS));
    //     calendarIntervals.add(new AggregateInterval(5, ChronoUnit.SECONDS));
    //     calendarIntervals.add(new AggregateInterval(10, ChronoUnit.SECONDS));
    //     calendarIntervals.add(new AggregateInterval(15, ChronoUnit.SECONDS));
    //     calendarIntervals.add(new AggregateInterval(30, ChronoUnit.SECONDS));
        
    //     // Minutes
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.MINUTES));
    //     calendarIntervals.add(new AggregateInterval(5, ChronoUnit.MINUTES));
    //     calendarIntervals.add(new AggregateInterval(10, ChronoUnit.MINUTES));
    //     calendarIntervals.add(new AggregateInterval(15, ChronoUnit.MINUTES));
    //     calendarIntervals.add(new AggregateInterval(20, ChronoUnit.MINUTES));
    //     calendarIntervals.add(new AggregateInterval(30, ChronoUnit.MINUTES));
        
    //     // Hours
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.HOURS));
    //     calendarIntervals.add(new AggregateInterval(2, ChronoUnit.HOURS));
    //     calendarIntervals.add(new AggregateInterval(3, ChronoUnit.HOURS));
    //     calendarIntervals.add(new AggregateInterval(4, ChronoUnit.HOURS));
    //     calendarIntervals.add(new AggregateInterval(6, ChronoUnit.HOURS));
    //     calendarIntervals.add(new AggregateInterval(12, ChronoUnit.HOURS));
        
    //     // Days and above
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.DAYS));
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.WEEKS));
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.MONTHS));
    //     calendarIntervals.add(new AggregateInterval(3, ChronoUnit.MONTHS));
    //     calendarIntervals.add(new AggregateInterval(6, ChronoUnit.MONTHS));
    //     calendarIntervals.add(new AggregateInterval(1, ChronoUnit.YEARS));

    //     long targetDurationMs = targetInterval.toDuration().toMillis();
        
    //     // Add all intervals that are smaller than or equal to the target interval
    //     // and are evenly divisible into the target interval
    //     for (AggregateInterval interval : calendarIntervals) {
    //         long intervalDurationMs = interval.toDuration().toMillis();
            
    //         if (intervalDurationMs <= targetDurationMs) {
    //             // For calendar-based intervals, we need special handling
    //             if (isCompatibleWithTarget(interval, targetInterval)) {
    //                 compatibleIntervals.add(interval);
    //             }
    //         } else {
    //             // Stop once we find an interval that exceeds targetIntervalMs
    //             break;
    //         }
    //     }
        
    //     return compatibleIntervals;
    // }
    
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
            case MINUTES:
                return smallerUnit == ChronoUnit.SECONDS && 
                       (60 % smallerMultiplier == 0) && 
                       (targetMultiplier % 1 == 0);
                       
            case HOURS:
                if (smallerUnit == ChronoUnit.MINUTES) {
                    return (60 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.SECONDS) {
                    return (3600 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                return false;
                
            case DAYS:
                if (smallerUnit == ChronoUnit.HOURS) {
                    return (24 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.MINUTES) {
                    return (1440 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                return false;
                
            case WEEKS:
                if (smallerUnit == ChronoUnit.DAYS) {
                    return (7 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                return false;
                
            case MONTHS:
                // This is approximate - months have varying lengths
                if (smallerUnit == ChronoUnit.DAYS) {
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                if (smallerUnit == ChronoUnit.WEEKS) {
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                return false;
                
            case YEARS:
                if (smallerUnit == ChronoUnit.MONTHS) {
                    return (12 % smallerMultiplier == 0) && 
                           (targetMultiplier % 1 == 0);
                }
                if (smallerUnit == ChronoUnit.DAYS) {
                    return smallerMultiplier == 1 && targetMultiplier % 1 == 0;
                }
                return false;
                
            default:
                return false;
        }
    }
}
