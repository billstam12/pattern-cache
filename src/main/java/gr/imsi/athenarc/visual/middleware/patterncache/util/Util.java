package gr.imsi.athenarc.visual.middleware.patterncache.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.patterncache.Sketch;

public class Util {
    /**
     * Generates a list of sketches, each representing one interval of the specified time unit
     * between the given timestamps.
     * 
     * @param from The start timestamp (epoch milliseconds)
     * @param to The end timestamp (epoch milliseconds)
     * @param timeUnit The time unit to divide the interval by (DAYS, HOURS, MINUTES, etc.)
     * @return A list of Sketch objects, each spanning exactly one interval of the specified time unit
     */
    public static List<Sketch> generateSketches(long from, long to, ChronoUnit chronoUnit) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Convert epoch milliseconds to Instant
        Instant startInstant = Instant.ofEpochMilli(from);
        Instant endInstant = Instant.ofEpochMilli(to);
        ZoneId zone = ZoneId.systemDefault();
                
        // Start with the truncated time unit (beginning of the time unit period)
        Instant currentInstant = startInstant
            .atZone(zone)
            .truncatedTo(chronoUnit)
            .toInstant();
        
        // Generate sketches for each time unit interval
        while (!currentInstant.isAfter(endInstant)) {
            Instant nextInstant = currentInstant.plus(1, chronoUnit);
            
            // If we're past the end time, use the end time as the boundary
            Instant intervalEnd = nextInstant.isAfter(endInstant) ? endInstant : nextInstant;
            
            sketches.add(new Sketch(
                currentInstant.toEpochMilli(),
                intervalEnd.toEpochMilli()
            ));
            
            currentInstant = nextInstant;
        }
        return sketches;
    }


    public static double computeSlope(Sketch sketch) {
        Stats stats = sketch.getStats();

        if(stats.getCount() == 0){
            return 0;
        }
        
        // Check if there are enough data points to calculate slope
        if (sketch.getDuration() < 2) {
            throw new IllegalArgumentException("Cannot compute slope with fewer than 2 data points. Found sketch with duration: " + sketch.getDuration());
        }
        
        DataPoint firstDataPoint = stats.getFirstDataPoint();
        DataPoint lastDataPoint = stats.getLastDataPoint();
        
        // Calculate slope based on first and last data points
        double valueChange = lastDataPoint.getValue() - firstDataPoint.getValue();
        
        double slope = valueChange / sketch.getDuration();
        
        // Using a simple normalization approach - could be refined based on expected slope ranges
        double normalizedSlope = Math.atan(slope) / Math.PI; // Maps to range [-0.5,0.5]
        
        return normalizedSlope;
    }
}
