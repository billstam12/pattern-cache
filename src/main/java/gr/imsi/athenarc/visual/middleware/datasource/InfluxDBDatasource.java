
package gr.imsi.athenarc.visual.middleware.datasource;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.slope.InfluxDBSlopeDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.domain.*;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class InfluxDBDatasource implements DataSource {

    InfluxDBQueryExecutor influxDBQueryExecutor;
    InfluxDBDataset dataset;
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDatasource.class);

    public InfluxDBDatasource(InfluxDBQueryExecutor influxDBQueryExecutor, InfluxDBDataset dataset) {
        this.dataset = dataset;
        this.influxDBQueryExecutor = influxDBQueryExecutor;
    }

    @Override
    public AbstractDataset getDataset() {
        return dataset;
    }

    @Override
    public AggregatedDataPoints getSlopeDataPoints(long from, long to, int measure, ChronoUnit chronoUnit) {
        return new InfluxDBSlopeDatapoints(from, to, measure, chronoUnit);        
    }


    final class InfluxDBSlopeDatapoints implements AggregatedDataPoints {

        private long from;
        private long to;
        private int measure;
        private ChronoUnit chronoUnit;

        public InfluxDBSlopeDatapoints(long from, long to, int measure, ChronoUnit chronoUnit) {
           this.from = from;
           this.to = to; 
           this.measure = measure;
           this.chronoUnit = chronoUnit;
        }

        @NotNull
        @Override
        public Iterator<AggregatedDataPoint> iterator() {
            String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
            TimeRange range = new TimeRange(from, to);
            int i = 0;
            String bucket = dataset.getSchema();
            String measurement = dataset.getTableName();
            String measureName = dataset.getHeader()[measure];
            String fluxTimeInterval = getFluxTimeInterval(chronoUnit);
            String fluxQuery =
               "customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every)\n" +
                "    |> fn(column:column)\n" +
                "    |> group()" +
                "\n" +
                "aggregate = (tables=<-, agg, name, aggregateInterval) => tables" +
                "\n" +
                "|> customAggregateWindow(every: aggregateInterval, fn: agg)" +
                "\n";

            fluxQuery += "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                    "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";

            fluxQuery += "union(\n" +
                    "    tables: [\n";

            fluxQuery += "data_" + i + "() |> aggregate(agg: first, name: \"data_" + i + "\"," + "aggregateInterval:" +  fluxTimeInterval + "),\n" +
                    "data_" + i + "() |> aggregate(agg: last, name: \"data_" + i + "\", " + "aggregateInterval:" + fluxTimeInterval + "),\n";
            
        
            fluxQuery+= "])\n";
            fluxQuery +=
                    "|> group(columns: [\"_field\", \"_start\", \"_stop\",])\n" +
                    "|> sort(columns: [\"_time\"], desc: false)\n";
            List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(fluxQuery);

            Map<String, Integer> measuresMap = new HashMap<>();
            measuresMap.put(measureName, measure);
            return new InfluxDBSlopeDataPointsIterator(fluxTables, measuresMap);
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
            return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));

        }
    }


    private String getFluxTimeInterval(ChronoUnit chronoUnit) {
        switch (chronoUnit) {
            case SECONDS:
                return "1s";
            case MINUTES:
                return "1m";    
            case HOURS:
                return "1h";
            case DAYS:
                return "1d";
            case WEEKS:
                return "1w";
            case MONTHS:
                return "1mo";
            case YEARS:
                return "1y";
            default:
                throw new IllegalArgumentException("Unsupported chrono unit: " + chronoUnit);
        }
    }

    public void closeConnection(){
        influxDBQueryExecutor.closeConnection();
    }

}