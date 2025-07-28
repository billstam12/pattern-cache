package gr.imsi.athenarc.middleware.datasource;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.InfluxDBAggregateDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

final class InfluxDBAggregatedDatapoints implements AggregatedDataPoints {

    private static final Set<String> SUPPORTED_AGGREGATE_FUNCTIONS = new HashSet<>(
        Arrays.asList("first", "last", "min", "max")
    );

    private AbstractDataset dataset;
    private InfluxDBQueryExecutor influxDBQueryExecutor;

    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure; 
    private Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;
    private Set<String> aggregateFunctions;

    public InfluxDBAggregatedDatapoints(InfluxDBQueryExecutor influxDBQueryExecutor, AbstractDataset dataset, 
                                     long from, long to,  Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, 
                                     Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure, Set<String> aggregateFunctions) {
       this.from = from;
       this.to = to; 
       this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
       this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
       this.dataset = dataset;
       this.influxDBQueryExecutor = influxDBQueryExecutor;
       
       // Validate aggregate functions
       if (aggregateFunctions == null || aggregateFunctions.isEmpty()) {
           throw new IllegalArgumentException("No aggregate functions specified");
       }
       
       // Check if all provided functions are supported
       for (String function : aggregateFunctions) {
           if (!SUPPORTED_AGGREGATE_FUNCTIONS.contains(function)) {
               throw new IllegalArgumentException("Unsupported aggregate function: " + function + 
                   ". Supported functions are: " + SUPPORTED_AGGREGATE_FUNCTIONS);
           }
       }
       
       this.aggregateFunctions = aggregateFunctions;
       
       // If no measures are specified, throw error
       if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.size() == 0 
            || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.size() == 0) {
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

        // Start building the query with the aggregate function definition
        StringBuilder fluxQuery = new StringBuilder();
        fluxQuery.append(
            "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
            "\n" +
            "|> aggregateWindow(every: aggregateInterval, createEmpty: true, offset: offset, fn: agg, timeSrc:\"_start\")" +
            "\n" +
            "|> map(fn: (r) => ({ r with agg: name }))" +
            "\n");

        // Generate data source definitions for each measure and interval
        int dataSourceCounter = 0;
        Map<String, Integer> measuresMap = new HashMap<>();
        
        // First, gather all data parts
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Use the global time range if no missing intervals are specified
                fluxQuery.append("data_").append(dataSourceCounter).append(" = () => from(bucket:\"")
                       .append(bucket).append("\")\n")
                       .append("|> range(start:").append(getFromDate(format)).append(", stop:")
                       .append(getToDate(format)).append(")\n")
                       .append("|> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\")\n")
                       .append("|> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\")\n");
                dataSourceCounter++;
            } else {
                // Add a data source for each missing interval
                for (TimeInterval interval : missingIntervals) {
                    String start = Instant.ofEpochMilli(interval.getFrom())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    String stop = Instant.ofEpochMilli(interval.getTo())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    
                    fluxQuery.append("data_").append(dataSourceCounter).append(" = () => from(bucket:\"")
                           .append(bucket).append("\")\n")
                           .append("|> range(start:").append(start).append(", stop:").append(stop).append(")\n")
                           .append("|> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\")\n")
                           .append("|> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\")\n");
                    dataSourceCounter++;
                }
            }
        }
        
        // Check if we need a union (only when there's more than one aggregate function)
        if (aggregateFunctions.size() > 1) {
            // Build the union for multiple aggregations
            fluxQuery.append("union(\n")
                   .append("    tables: [\n");
            
            // Reset counter for second pass
            dataSourceCounter = 0;
            
            // Second pass - create aggregations for all data sources
            for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
                AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
                String measureFluxTimeInterval = getFluxTimeInterval(aggregateInterval);
                List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
                
                if (missingIntervals == null || missingIntervals.isEmpty()) {
                    // Calculate offset based on the from time
                    long offset = from % (aggregateInterval.getMultiplier() * 
                                         getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                    
                    // Add all specified aggregation functions
                    for (String aggFunction : aggregateFunctions) {
                        fluxQuery.append("        data_").append(dataSourceCounter)
                                .append("() |> aggregate(agg: ").append(aggFunction)
                                .append(", name: \"").append(aggFunction).append("\", offset: ")
                                .append(offset).append("ms,")
                                .append("aggregateInterval:").append(measureFluxTimeInterval).append("),\n");
                    }
                    
                    dataSourceCounter++;
                } else {
                    // Process each missing interval
                    for (TimeInterval interval : missingIntervals) {
                        // Calculate offset based on the interval start time
                        long offset = interval.getFrom() % (aggregateInterval.getMultiplier() * 
                                                         getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                        
                        // Add all specified aggregation functions for each interval
                        for (String aggFunction : aggregateFunctions) {
                            fluxQuery.append("        data_").append(dataSourceCounter)
                                    .append("() |> aggregate(agg: ").append(aggFunction)
                                    .append(", name: \"").append(aggFunction).append("\", offset: ")
                                    .append(offset).append("ms,")
                                    .append("aggregateInterval:").append(measureFluxTimeInterval).append("),\n");
                        }
                        
                        dataSourceCounter++;
                    }
                }
            }
            
            // Remove the trailing comma and close the union
            fluxQuery.delete(fluxQuery.length() - 2, fluxQuery.length());
            fluxQuery.append("\n    ]\n)");
        } else {
            // For single aggregate function, avoid using union
            String aggFunction = aggregateFunctions.iterator().next();
            
            // Direct aggregation without union
            fluxQuery.append("data_0() |> aggregate(agg: ").append(aggFunction)
                    .append(", name: \"").append(aggFunction).append("\", offset: ");
            
            // Get the first measure's interval for simplicity
            // (This is fine since we typically use the same aggregation interval for all measures)
            int firstMeasureIdx = missingIntervalsPerMeasure.keySet().iterator().next();
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(firstMeasureIdx);
            String measureFluxTimeInterval = getFluxTimeInterval(aggregateInterval);
            
            // Calculate appropriate offset
            long offset = from % (aggregateInterval.getMultiplier() * 
                                 getChronoUnitMillis(aggregateInterval.getChronoUnit()));
            
            fluxQuery.append(offset).append("ms,")
                    .append("aggregateInterval:").append(measureFluxTimeInterval).append(")");
            
            // Additional measures need to be processed with join operations if there are multiple measures
            int remainingMeasures = dataSourceCounter - 1;
            for (int i = 1; i <= remainingMeasures; i++) {
                fluxQuery.append("\n|> union(tables: data_").append(i).append("() |> aggregate(agg: ")
                        .append(aggFunction).append(", name: \"").append(aggFunction).append("\", offset: ")
                        .append(offset).append("ms,")
                        .append("aggregateInterval:").append(measureFluxTimeInterval).append("))");
            }
        }
        
        // Add final operations for grouping and sorting
        fluxQuery.append("\n|> group(columns: [\"_field\", \"_start\", \"_stop\"])")
                .append("\n|> sort(columns: [\"_time\"], desc: false)\n");
        
        // Execute the query
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery.toString());
        return new InfluxDBAggregateDataPointsIterator(fluxTables, measuresMap, aggregateFunctions.size());
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
