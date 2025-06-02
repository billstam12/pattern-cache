package gr.imsi.athenarc.middleware.datasource;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.iterator.InfluxDBSlopeAggregatedDataPointIterator;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

public class InfluxDBSlopeAggregatedDataPoints implements AggregatedDataPoints {

    private final InfluxDBQueryExecutor influxDBQueryExecutor;
    private final InfluxDBDataset dataset;
    private final long from;
    private final long to;
    private final Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    public InfluxDBSlopeAggregatedDataPoints(InfluxDBQueryExecutor influxDBQueryExecutor, InfluxDBDataset dataset,
            long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        this.influxDBQueryExecutor = influxDBQueryExecutor;
        this.dataset = dataset;
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;
        
        // Validate parameters
        if (this.missingIntervalsPerMeasure == null || this.missingIntervalsPerMeasure.isEmpty() 
            || aggregateIntervalsPerMeasure == null || aggregateIntervalsPerMeasure.isEmpty()) {
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

        // Start building the query - we'll use Flux's math library
        StringBuilder fluxQuery = new StringBuilder();
        fluxQuery.append("import \"math\"\n");
        
        // Generate data source definitions for each measure and interval
        int dataSourceCounter = 0;
        Map<String, Integer> measuresMap = new HashMap<>();

        for (int measureIdx : missingIntervalsPerMeasure.keySet()) {
            String measureName = headers[measureIdx];
            measuresMap.put(measureName, measureIdx);
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIdx);
            String windowInterval = getFluxTimeInterval(aggregateInterval);
            List<TimeInterval> missingIntervals = missingIntervalsPerMeasure.get(measureIdx);

            if (missingIntervals == null || missingIntervals.isEmpty()) {
                // Use the global time range if no missing intervals are specified
                String fromDate = getFromDate(format);
                String toDate = getToDate(format);
                
                fluxQuery.append("from(bucket: \"").append(bucket).append("\")\n")
                       .append("|> range(start: ").append(fromDate).append(", stop: ")
                       .append(toDate).append(")\n")
                       .append("|> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\")\n")
                       .append("|> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\")\n")
                       .append("|> window(every: ").append(windowInterval).append(")\n")
                       .append("|> map(fn: (r) => ({\n")
                       .append("    r with\n")
                       .append("    x_norm:\n") 
                       .append("        (float(v: uint(v: r._time)) - float(v: uint(v: r._start)))")
                       .append("        / (float(v: uint(v: r._stop)) - float(v: uint(v: r._start)))\n")
                       .append("}))\n")   
                       .append("|> reduce(\n")
                       .append("    fn: (r, accumulator) => ({\n")
                       .append("        sum_x: accumulator.sum_x + r.x_norm,\n")
                       .append("        sum_y: accumulator.sum_y + r._value,\n")
                       .append("        sum_xy: accumulator.sum_xy + (r.x_norm * r._value),\n")
                       .append("        sum_x2: accumulator.sum_x2 + math.pow(x: r.x_norm, y: 2.0),\n")
                       .append("        count: accumulator.count + 1,\n")
                       .append("    }),\n")
                       .append("    identity: {sum_x: 0.0, sum_y: 0.0, sum_xy: 0.0, sum_x2: 0.0, count: 0}\n")
                       .append(")\n");
                dataSourceCounter++;
            } else {
                // Process each missing interval with a separate query part
                for (TimeInterval interval : missingIntervals) {
                    String startDate = Instant.ofEpochMilli(interval.getFrom())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    String stopDate = Instant.ofEpochMilli(interval.getTo())
                            .atZone(ZoneId.of("UTC"))
                            .format(DateTimeFormatter.ofPattern(format));
                    
                    if (dataSourceCounter > 0) {
                        fluxQuery.append("union(tables: [data_").append(dataSourceCounter - 1).append(", ");
                    }
                    
                    fluxQuery.append("from(bucket: \"").append(bucket).append("\")\n")
                            .append("|> range(start: ").append(startDate).append(", stop: ")
                            .append(stopDate).append(")\n")
                            .append("|> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\")\n")
                            .append("|> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\")\n")
                            .append("|> window(every: ").append(windowInterval).append(")\n")
                            .append("|> map(fn: (r) => ({\n")
                            .append("    r with\n")
                            .append("    x_norm:\n") 
                            .append("        (float(v: uint(v: r._time)) - float(v: uint(v: r._start)))")
                            .append("        / (float(v: uint(v: r._stop)) - float(v: uint(v: r._start)))\n")
                            .append("}))\n")     
                            .append("|> reduce(\n")
                            .append("    fn: (r, accumulator) => ({\n")
                            .append("        sum_x: accumulator.sum_x + r.x_norm,\n")
                            .append("        sum_y: accumulator.sum_y + r._value,\n")
                            .append("        sum_xy: accumulator.sum_xy + (r.x_norm * r._value),\n")
                            .append("        sum_x2: accumulator.sum_x2 + math.pow(x: r.x_norm, y: 2.0),\n")
                            .append("        count: accumulator.count + 1,\n")
                            .append("    }),\n")
                            .append("    identity: {sum_x: 0.0, sum_y: 0.0, sum_xy: 0.0, sum_x2: 0.0, count: 0}\n")
                            .append(")\n");
                           
                    if (dataSourceCounter > 0) {
                        fluxQuery.append("])\n");
                    }
                           
                    dataSourceCounter++;
                }
            }
        }
        // Execute the query
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery.toString());
        
        // Return an iterator that processes these results using our updated iterator class
        return new InfluxDBSlopeAggregatedDataPointIterator(fluxTables, measuresMap);
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }
}