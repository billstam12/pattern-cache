package gr.imsi.athenarc.middleware.manager.pattern.util;

import java.util.ArrayList;
import java.util.List;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.manager.pattern.Sketch;

public class Util {
   
    /**
     * Generate sketches covering the specified time range based on the AggregateInterval.
     * Ensures the sketches are properly aligned with chronological boundaries.
     * 
     * @param from Start timestamp (already aligned to time unit boundary)
     * @param to End timestamp (already aligned to time unit boundary)
     * @param timeUnit Aggregate interval for sketches
     * @return List of sketches spanning the time range
     */
    public static List<Sketch> generateAlignedSketches(long from, long to, AggregateInterval timeUnit, AggregationType aggregationType) {
        List<Sketch> sketches = new ArrayList<>();
        
        // Calculate the number of complete intervals
        long unitDurationMs = timeUnit.toDuration().toMillis();
        int numIntervals = (int) Math.ceil((double)(to - from) / unitDurationMs);
        
        // Create a sketch for each interval
        for (int i = 0; i < numIntervals; i++) {
            long sketchStart = from + (i * unitDurationMs);
            long sketchEnd = Math.min(sketchStart + unitDurationMs, to);
            sketches.add(new Sketch(sketchStart, sketchEnd, aggregationType));
        }
        
        return sketches;
    }

    public static Sketch combineSketches(List<Sketch> sketchs){
        if(sketchs == null || sketchs.isEmpty()){
            throw new IllegalArgumentException("Cannot combine empty list of sketches");
        }
        Sketch firstSketch = sketchs.get(0);
        Sketch combinedSketch = firstSketch.clone();

        for(int i = 1; i < sketchs.size(); i++){
            combinedSketch.combine(sketchs.get(i));;
        }
        return combinedSketch;
    }

}
