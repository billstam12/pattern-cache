package gr.imsi.athenarc.middleware.datasource;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.InfluxDBDataPointsIterator;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

final class InfluxDBDataPoints implements DataPoints {
    
    private AbstractDataset dataset;
    private InfluxDBQueryExecutor influxDBQueryExecutor;

    private long from;
    private long to;
    private Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;

    public InfluxDBDataPoints(InfluxDBQueryExecutor influxDBQueryExecutor, AbstractDataset dataset, 
                           long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.dataset = dataset;
        this.influxDBQueryExecutor = influxDBQueryExecutor;
        
        // If no measures are specified, throw error
        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.size() == 0) {
            throw new IllegalArgumentException("No measures specified");
        }
    }

    @NotNull
    @Override
    public Iterator<DataPoint> iterator() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String bucket = dataset.getSchema();
        String measurement = dataset.getTableName();
        String[] headers = dataset.getHeader();
        
        StringBuilder fluxQuery = new StringBuilder();
        
        // Create a query for each measure
        List<String> queryParts = new java.util.ArrayList<>();
        int i = 0;
        Map<String, Integer> measuresMap = new HashMap<>();
        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);
            
            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Use the global time range if no missing intervals are specified
                String dataPart = "data_" + i + " = from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + getFromDate(format) + ", stop:" + getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                
                fluxQuery.append(dataPart);
                queryParts.add("data_" + i);
            } else if (missingIntervals.size() == 1) {
                // Special case for a single missing interval
                TimeInterval interval = missingIntervals.get(0);
                String start = Instant.ofEpochMilli(interval.getFrom())
                        .atZone(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(format));
                String stop = Instant.ofEpochMilli(interval.getTo())
                        .atZone(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(format));
                
                String dataPart = "data_" + i + " = from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + start + ", stop:" + stop + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                
                fluxQuery.append(dataPart);
                queryParts.add("data_" + i);
            } else {
                // Generate a union of queries for multiple missing intervals
                fluxQuery.append("data_").append(i).append(" = union(\n")
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
                queryParts.add("data_" + i);
            }
            i++;
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
        fluxQuery.append("\n|> group(columns: [\"_field\"])\n")
                .append("|> sort(columns: [\"_time\"], desc: false)\n");
        
        // Execute the query
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery.toString());
        return new InfluxDBDataPointsIterator(fluxTables, measuresMap);
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