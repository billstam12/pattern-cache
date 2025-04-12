package gr.imsi.athenarc.visual.middleware.manager.pattern.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import gr.imsi.athenarc.visual.middleware.manager.pattern.Sketch;

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

    public static Sketch combineSketches(List<Sketch> sketchs){
        if(sketchs == null || sketchs.isEmpty()){
            throw new IllegalArgumentException("Cannot combine empty list of sketches");
        }
        Sketch firstSketch = sketchs.get(0);
        Sketch combinedSketch = new Sketch(firstSketch.getFrom(), firstSketch.getTo());
        combinedSketch.addAggregatedDataPoint(firstSketch);

        for(int i = 1; i < sketchs.size(); i++){
            combinedSketch.combine(sketchs.get(i));;
        }
        return combinedSketch;
    }

}
