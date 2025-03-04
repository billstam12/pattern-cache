package gr.imsi.athenarc.visual.middleware.util;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class QueryResultComparator {
    private static final Logger LOG = LoggerFactory.getLogger(QueryResultComparator.class);
    private static final double EPSILON = 0.000001;

    public static boolean compareResults(Iterator<DataPoint> influxResults, Iterator<DataPoint> postgresResults) {
        List<String> differences = new ArrayList<>();
        int count = 0;
        
        while (influxResults.hasNext() && postgresResults.hasNext()) {
            count++;
            DataPoint influxPoint = influxResults.next();
            DataPoint postgresPoint = postgresResults.next();
            
            if (!arePointsEqual(influxPoint, postgresPoint)) {
                differences.add(String.format("Difference at point %d:\nInflux: %s\nPostgres: %s", 
                    count, formatDataPoint(influxPoint), formatDataPoint(postgresPoint)));
            }
        }

        // Check if one iterator has more elements than the other
        boolean hasRemainingElements = checkRemainingElements(influxResults, postgresResults, differences, count);
        
        if (!differences.isEmpty()) {
            LOG.error("Found {} differences in results:", differences.size());
            differences.forEach(diff -> LOG.error(diff));
            return false;
        }

        LOG.info("Successfully compared {} points with no differences", count);
        return !hasRemainingElements;
    }

    private static boolean arePointsEqual(DataPoint p1, DataPoint p2) {
        return p1.getTimestamp() == p2.getTimestamp() &&
               p1.getMeasure() == p2.getMeasure() &&
               Math.abs(p1.getValue() - p2.getValue()) < EPSILON;
    }

    private static String formatDataPoint(DataPoint point) {
        return String.format("(timestamp=%d, measure=%d, value=%.6f)",
            point.getTimestamp(), point.getMeasure(), point.getValue());
    }

    private static boolean checkRemainingElements(
            Iterator<DataPoint> iter1, 
            Iterator<DataPoint> iter2, 
            List<String> differences,
            int count) {
        
        boolean hasRemaining = false;
        
        if (iter1.hasNext()) {
            differences.add("InfluxDB has more points than PostgreSQL");
            hasRemaining = true;
        }
        
        if (iter2.hasNext()) {
            differences.add("PostgreSQL has more points than InfluxDB");
            hasRemaining = true;
        }
        
        return hasRemaining;
    }
}
