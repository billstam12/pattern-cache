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
import gr.imsi.athenarc.middleware.datasource.iterator.InfluxDBM4DataPointsIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

final class InfluxDBM4Datapoints implements AggregatedDataPoints {

    private AbstractDataset dataset;
    private InfluxDBQueryExecutor influxDBQueryExecutor;

    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure; 
    private Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    public InfluxDBM4Datapoints(InfluxDBQueryExecutor influxDBQueryExecutor, AbstractDataset dataset, 
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
        aggregateFunctions.add("first");
        aggregateFunctions.add("last");
        
        // Start building the query with the custom function definitions
        StringBuilder fluxQuery = new StringBuilder();
        fluxQuery.append("customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", offset, tables=<-) =>\n" +
            "  tables\n" +
            "    |> window(every:every, offset: offset, createEmpty: true)\n" +
            "    |> fn(column:column)\n" +
            "    |> group()" +
            "\n" +
            "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
            "\n" +
            "|> customAggregateWindow(every: aggregateInterval, fn: agg, offset: offset)" +
            "\n" +
            "|> map(fn: (r) => ({ r with agg: name }))" +
            "\n");

        // Generate data section for each measure and its time intervals
        int dataSourceCounter = 0;
        Map<String, Integer> measuresMap = new HashMap<>();
        
        // First pass - gather all data source parts
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            
            // If no specific intervals were provided, use the full time range
            if (missingIntervals == null || missingIntervals.isEmpty()) {
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
        
        // Start building the union of all data with all aggregation functions
        fluxQuery.append("union(\n")
               .append("    tables: [\n");
        
        // Reset counter for second pass
        dataSourceCounter = 0;
        
        // Second pass - create the union of all aggregations
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            String measureFluxTimeInterval = getFluxTimeInterval(aggregateInterval);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            
            // If no specific intervals were provided, use the full time range
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Calculate offset based on the from time
                long offset = from % (aggregateInterval.getMultiplier() * 
                                     getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                
                for (String aggregateFunction : aggregateFunctions) {
                    fluxQuery.append("        data_").append(dataSourceCounter).append("() |> aggregate(agg: ")
                            .append(aggregateFunction).append(", name: \"").append(aggregateFunction).append("\", ")
                            .append("aggregateInterval:").append(measureFluxTimeInterval)
                            .append(", offset: ").append(offset).append("ms")
                            .append("),\n");
                }
                dataSourceCounter++;
            } else {
                // Add aggregations for each missing interval
                for (TimeInterval interval : missingIntervals) {
                    // Calculate offset based on the interval start time
                    long offset = interval.getFrom() % (aggregateInterval.getMultiplier() * 
                                                     getChronoUnitMillis(aggregateInterval.getChronoUnit()));
                    
                    for (String aggregateFunction : aggregateFunctions) {
                        fluxQuery.append("        data_").append(dataSourceCounter).append("() |> aggregate(agg: ")
                                .append(aggregateFunction).append(", name: \"").append(aggregateFunction).append("\", ")
                                .append("aggregateInterval:").append(measureFluxTimeInterval)
                                .append(", offset: ").append(offset).append("ms")
                                .append("),\n");
                    }
                    dataSourceCounter++;
                }
            }
        }
        
        // Remove the trailing comma and close the union
        fluxQuery.delete(fluxQuery.length() - 2, fluxQuery.length());
        fluxQuery.append("\n    ]\n)");
        
        // Add final operations for sorting
        fluxQuery.append("\n|> group(columns: [\"_field\"])")
                .append("\n|> sort(columns: [\"_time\"], desc: false)\n");
        
        // Execute the query
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery.toString());
        return new InfluxDBM4DataPointsIterator(fluxTables, measuresMap);
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
