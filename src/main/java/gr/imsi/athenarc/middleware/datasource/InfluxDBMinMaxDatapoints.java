package gr.imsi.athenarc.middleware.datasource;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import org.jetbrains.annotations.NotNull;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.InfluxDBMinMaxDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

final class InfluxDBMinMaxDatapoints implements AggregatedDataPoints {

    private AbstractDataset dataset;
    private InfluxDBQueryExecutor influxDBQueryExecutor;

    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure; 
    private Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    public InfluxDBMinMaxDatapoints(InfluxDBQueryExecutor influxDBQueryExecutor, AbstractDataset dataset, 
                                     long from, long to,  Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, 
                                     Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure
                                    ) {
       this.from = from;
       this.to = to; 
       this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
       this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
       this.dataset = dataset;
       this.influxDBQueryExecutor = influxDBQueryExecutor;
       
       // If no measures are specified, throw error
       if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.size() == 0 
            || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.size() == 0 || aggregateIntervalsPerMeasure.size() == 0) {
           throw new IllegalArgumentException("No measures specified");
       }
    }

    private String getFluxTimeInterval(AggregateInterval aggregateInterval) {
        ChronoUnit chronoUnit = aggregateInterval.getChronoUnit();
        long multiplier = aggregateInterval.getMultiplier();
        switch (chronoUnit) {
            case MILLIS:
                return multiplier + "ms";
            case SECONDS:
                return multiplier + "s";
            case MINUTES:
                return multiplier + "m";    
            case HOURS:
                return multiplier + "h";
            case DAYS:
                return multiplier + "d";
            case WEEKS:
                return multiplier + "w";
            case MONTHS:
                return multiplier + "mo";
            case YEARS:
                return multiplier + "y";
            default:
                throw new IllegalArgumentException("Unsupported chrono unit: " + chronoUnit);
        }
    }

    @NotNull
    @Override
    public Iterator<AggregatedDataPoint> iterator() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String bucket = dataset.getSchema();
        String measurement = dataset.getTableName();
        String[] headers = dataset.getHeader();
        Set<String> aggregateFunctions = new HashSet<>();
        aggregateFunctions.add("min");
        aggregateFunctions.add("max");

        StringBuilder fluxQuery = new StringBuilder();
        fluxQuery.append(
            "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
            "\n" +
            "|> aggregateWindow(every: aggregateInterval, fn: agg, offset: offset)" +
            "\n");

        // Create a query for each measure
        List<String> queryParts = new java.util.ArrayList<>();
        int i = 0;
        Map<String, Integer> measuresMap = new HashMap<>();
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            // Get the specific aggregate interval for this measure
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            String measureFluxTimeInterval = getFluxTimeInterval(aggregateInterval);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Use the global time range if no missing intervals are specified
                String dataPart = "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + getFromDate(format) + ", stop:" + getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                
                fluxQuery.append(dataPart);
                
                // Calculate offset based on the from time
                long offset = from % (aggregateInterval.getMultiplier() * 
                                     getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                
                StringBuilder unionPart = new StringBuilder();
                unionPart.append("union_").append(i).append(" = union(\n")
                       .append("    tables: [\n");
                
                for (String aggregateFunction : aggregateFunctions) {
                    unionPart.append("        data_").append(i).append("() |> aggregate(agg: ")
                            .append(aggregateFunction).append(", name: \"data_").append(i).append("\", ")
                            .append("aggregateInterval:").append(measureFluxTimeInterval)
                            .append(", offset: ").append(offset).append("ms")
                            .append("),\n");
                }
                
                String unionQueries = unionPart.substring(0, unionPart.length() - 2);
                unionPart = new StringBuilder(unionQueries);
                unionPart.append("\n    ]\n)\n");
                
                fluxQuery.append(unionPart);
                queryParts.add("union_" + i);
                
            } else if (missingIntervals.size() == 1) {
                // Special case for a single missing interval
                TimeInterval interval = missingIntervals.get(0);
                String start = Instant.ofEpochMilli(interval.getFrom())
                        .atZone(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(format));
                String stop = Instant.ofEpochMilli(interval.getTo())
                        .atZone(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(format));
                
                String dataPart = "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + start + ", stop:" + stop + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                
                fluxQuery.append(dataPart);
                
                // Calculate offset based on the interval start time
                long offset = interval.getFrom() % (aggregateInterval.getMultiplier() * 
                                                  getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                
                StringBuilder unionPart = new StringBuilder();
                unionPart.append("union_").append(i).append(" = union(\n")
                       .append("    tables: [\n");
                
                for (String aggregateFunction : aggregateFunctions) {
                    unionPart.append("        data_").append(i).append("() |> aggregate(agg: ")
                            .append(aggregateFunction).append(", name: \"data_").append(i).append("\", ")
                            .append("aggregateInterval:").append(measureFluxTimeInterval)
                            .append(", offset: ").append(offset).append("ms")
                            .append("),\n");
                }
                
                String unionQueries = unionPart.substring(0, unionPart.length() - 2);
                unionPart = new StringBuilder(unionQueries);
                unionPart.append("\n    ]\n)\n");
                
                fluxQuery.append(unionPart);
                queryParts.add("union_" + i);
                
            } else {
                // Generate a union of queries for multiple missing intervals
                fluxQuery.append("data_").append(i).append(" = () => union(\n")
                       .append("    tables: [\n");
                
                for (int j = 0; j < missingIntervals.size(); j++) {
                    TimeInterval interval = missingIntervals.get(j);
                    String start = Instant.ofEpochMilli(interval.getFrom())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    String stop = Instant.ofEpochMilli(interval.getTo())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    
                    fluxQuery.append("        from(bucket:\"").append(bucket).append("\")\n")
                            .append("        |> range(start:").append(start).append(", stop:").append(stop).append(")\n")
                            .append("        |> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\")\n")
                            .append("        |> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\")");
                    
                    if (j < missingIntervals.size() - 1) {
                        fluxQuery.append(",\n");
                    } else {
                        fluxQuery.append("\n");
                    }
                }
                
                fluxQuery.append("    ]\n)\n");
                
                StringBuilder unionPart = new StringBuilder();
                unionPart.append("union_").append(i).append(" = union(\n")
                       .append("    tables: [\n");
                
                for (String aggregateFunction : aggregateFunctions) {
                    // Get the first interval's start time for offset calculation
                    long offset = missingIntervals.get(0).getFrom() % (aggregateInterval.getMultiplier() * 
                                                                     getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                    
                    unionPart.append("        data_").append(i).append("() |> aggregate(agg: ")
                            .append(aggregateFunction).append(", name: \"data_").append(i).append("\", ")
                            .append("aggregateInterval:").append(measureFluxTimeInterval)
                            .append(", offset: ").append(offset).append("ms")
                            .append("),\n");
                }
                
                String unionQueries = unionPart.substring(0, unionPart.length() - 2);
                unionPart = new StringBuilder(unionQueries);
                unionPart.append("\n    ]\n)\n");
                
                fluxQuery.append(unionPart);
                queryParts.add("union_" + i);
            }
            i ++;
        }
        
        // Combine all measure queries using union if we have multiple measures
        if (missingIntervalsPerMeasure.size() > 1) {
            fluxQuery.append("combined = union(\n")
                   .append("    tables: [");
            
            for (String part : queryParts) {
                fluxQuery.append(part).append(", ");
            }
            
            // Remove the trailing comma and space
            fluxQuery.delete(fluxQuery.length() - 2, fluxQuery.length());
            fluxQuery.append("]\n)\n");
            
            fluxQuery.append("combined");
        } else {
            fluxQuery.append(queryParts.get(0));
        }
        
        // Add final operations
        fluxQuery.append("\n|> group(columns: [\"_field\", \"_start\", \"_stop\"])\n")
                .append("|> sort(columns: [\"_time\"], desc: false)\n");
        
        // Execute the query
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery.toString());
        return new InfluxDBMinMaxDataPointsIterator(fluxTables, measuresMap);
    }

    /**
     * Helper method to convert ChronoUnit to milliseconds
     */
    private long getChronoUnitMillis(ChronoUnit unit) {
        switch (unit) {
            case MILLIS:
                return 1;
            case SECONDS:
                return 1000;
            case MINUTES:
                return 60 * 1000;
            case HOURS:
                return 60 * 60 * 1000;
            case DAYS:
                return 24 * 60 * 60 * 1000;
            case WEEKS:
                return 7 * 24 * 60 * 60 * 1000;
            case MONTHS:
                return 30L * 24L * 60L * 60L * 1000L; // Approximate
            case YEARS:
                return 365L * 24L * 60L * 60L * 1000L; // Approximate
            default:
                throw new IllegalArgumentException("Unsupported ChronoUnit: " + unit);
        }
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return "";
    }

    @Override
    public String getToDate() {
        return "";
    }

    @Override
    public String getFromDate(String format) {
        return DateTimeUtil.format(from, format);
    }

    @Override
    public String getToDate(String format) {
        return DateTimeUtil.format(to, format);
    }
}
