package gr.imsi.athenarc.visual.middleware.datasource;

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

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.InfluxDBAggregatedDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.domain.MeasureAggregationRequest;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

final class InfluxDBAggregatedDatapoints implements AggregatedDataPoints {

    private AbstractDataset dataset;
    private InfluxDBQueryExecutor influxDBQueryExecutor;

    private long from;
    private long to;
    private List<MeasureAggregationRequest> measureAggregationRequests;
    private Set<String> aggregateFunctions;

    public InfluxDBAggregatedDatapoints(InfluxDBQueryExecutor influxDBQueryExecutor, AbstractDataset dataset, 
                                     long from, long to, List<MeasureAggregationRequest> measureAggregationRequests,
                                     Set<String> aggregateFunctions) {
       this.from = from;
       this.to = to; 
       this.measureAggregationRequests = measureAggregationRequests;
       this.dataset = dataset;
       this.influxDBQueryExecutor = influxDBQueryExecutor;
       this.aggregateFunctions = aggregateFunctions != null ? aggregateFunctions : new HashSet<>();
       
       // If no aggregate functions are specified, throw error
       if (this.aggregateFunctions.isEmpty()) {
           throw new IllegalArgumentException("No aggregate functions specified");
       }
       
       // If no measures are specified, throw error
       if (this.measureAggregationRequests == null || this.measureAggregationRequests.size() == 0) {
           throw new IllegalArgumentException("No measures specified");
       }
    }

    private String getFluxTimeInterval(AggregateInterval aggregateInterval) {
        ChronoUnit chronoUnit = aggregateInterval.getChronoUnit();
        int multiplier = aggregateInterval.getMultiplier();
        switch (chronoUnit) {
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
        
        StringBuilder fluxQuery = new StringBuilder();
        fluxQuery.append("customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
            "  tables\n" +
            "    |> window(every:every)\n" +
            "    |> fn(column:column)\n" +
            "    |> group()" +
            "\n" +
            "aggregate = (tables=<-, agg, name, aggregateInterval) => tables" +
            "\n" +
            "|> customAggregateWindow(every: aggregateInterval, fn: agg)" +
            "\n");

        // Create a query for each measure
        List<String> queryParts = new java.util.ArrayList<>();
        
        for (int i = 0; i < measureAggregationRequests.size(); i++) {
            MeasureAggregationRequest request = measureAggregationRequests.get(i);
            int measureIdx = request.getMeasureIndex();
            String measureName = headers[measureIdx];
            // Get the specific aggregate interval for this measure
            String measureFluxTimeInterval = getFluxTimeInterval(request.getAggregateInterval());
            List<TimeInterval> missingIntervals = request.getMissingIntervals();
            
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Use the global time range if no missing intervals are specified
                String dataPart = "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + getFromDate(format) + ", stop:" + getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                
                fluxQuery.append(dataPart);
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
            }
            
            if (aggregateFunctions.size() > 1) {
                // Multiple aggregate functions for this measure
                StringBuilder unionPart = new StringBuilder();
                unionPart.append("union_").append(i).append(" = union(\n")
                       .append("    tables: [\n");
                
                // Add each aggregate function to the query
                for (String aggregateFunction : aggregateFunctions) {
                    unionPart.append("        data_").append(i).append("() |> aggregate(agg: ")
                            .append(aggregateFunction).append(", name: \"data_").append(i).append("\", ")
                            .append("aggregateInterval:").append(measureFluxTimeInterval).append("),\n");
                }
                
                // Remove the trailing comma and newline
                String unionQueries = unionPart.substring(0, unionPart.length() - 2);
                unionPart = new StringBuilder(unionQueries);
                unionPart.append("\n    ]\n)\n");
                
                fluxQuery.append(unionPart);
                queryParts.add("union_" + i);
            } else {
                // Single aggregate function for this measure
                String aggregateFunction = aggregateFunctions.iterator().next();
                String singleAggregatePart = "single_" + i + " = data_" + i + "() |> aggregate(agg: " + aggregateFunction + 
                        ", name: \"data_" + i + "\", aggregateInterval:" + measureFluxTimeInterval + ")\n";
                
                fluxQuery.append(singleAggregatePart);
                queryParts.add("single_" + i);
            }
        }
        
        // Combine all measure queries using union if we have multiple measures
        if (measureAggregationRequests.size() > 1) {
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
        Map<String, Integer> measuresMap = new HashMap<>();
        for (int i = 0; i < measureAggregationRequests.size(); i++) {
            MeasureAggregationRequest request = measureAggregationRequests.get(i);
            int measureIdx = request.getMeasureIndex();
            String measureName = dataset.getHeader()[measureIdx];
            measuresMap.put(measureName, i);
        }
        return new InfluxDBAggregatedDataPointsIterator(fluxTables, measuresMap, aggregateFunctions.size());
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
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }
}
