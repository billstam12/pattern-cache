package gr.imsi.athenarc.visual.middleware.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


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
 
    public static int numberOfIntervals(final long startTime, final long endTime, long aggregateInterval) {
        return (int) Math.ceil((double)(endTime - startTime) / aggregateInterval);
    }

    public static int indexInInterval(final long startTime, final long endTime, final long aggregateInterval, final long time) {
        return (int) ((time - startTime ) / aggregateInterval);
    }
}
