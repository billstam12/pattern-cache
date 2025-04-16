package gr.imsi.athenarc.middleware.datasource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.config.*;
import gr.imsi.athenarc.middleware.datasource.connection.*;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.datasource.executor.*;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeRange;

public class DataSourceFactory {
    
    public static DataSource createDataSource(DataSourceConfiguration config) {
        if (config instanceof InfluxDBConfiguration) {
            return createInfluxDBDataSource((InfluxDBConfiguration) config);
        }
        throw new IllegalArgumentException("Unsupported data source configuration");
    }

    private static DataSource createInfluxDBDataSource(InfluxDBConfiguration config) {
        InfluxDBConnection connection = (InfluxDBConnection) new InfluxDBConnection(
            config.getUrl(), 
            config.getOrg(), 
            config.getToken(), 
            config.getBucket()
        ).connect();
        InfluxDBQueryExecutor executor = new InfluxDBQueryExecutor(connection);

        InfluxDBDataset dataset = new InfluxDBDataset(
            config.getMeasurement(),
            config.getBucket(),
            config.getMeasurement()
        );

        fillInfluxDBDatasetInfo(dataset, executor);

        return new InfluxDBDatasource(executor, dataset);
    }

    private static void fillInfluxDBDatasetInfo(InfluxDBDataset dataset, InfluxDBQueryExecutor influxDBQueryExecutor) {
        // Fetch first timestamp
        String firstQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> first()\n";
    
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(firstQuery);
        FluxRecord firstRecord = fluxTables.get(0).getRecords().get(0);
        long firstTime = firstRecord.getTime().toEpochMilli();
    
        // Fetch last timestamp
        String lastQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> last()\n";
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(lastQuery);
        FluxRecord lastRecord = fluxTables.get(0).getRecords().get(0);
        long lastTime = lastRecord.getTime().toEpochMilli();
    
        String influxFormat = "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"";
        // Fetch the second timestamp to calculate the sampling interval.
        // Query on first time plus some time later
        String secondQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start:" + DateTimeUtil.format(firstTime, influxFormat).replace("\"", "") + 
            ", stop: " + DateTimeUtil.format(firstTime + 360000, influxFormat).replace("\"", "") + ")\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> limit(n: 2)\n";  // Fetch the first two records
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(secondQuery);
        FluxRecord secondRecord = fluxTables.get(0).getRecords().get(1); 
        long secondTime = secondRecord.getTime().toEpochMilli();
    
        // Calculate and set sampling interval
        dataset.setSamplingInterval(secondTime - firstTime);
    
        // Set time range and headers
        dataset.setTimeRange(new TimeRange(firstTime, lastTime));
    
        // Populate header (field keys)
        Set<String> header = fluxTables.stream()
            .flatMap(fluxTable -> fluxTable.getRecords().stream())
            .map(FluxRecord::getField)
            .collect(Collectors.toSet());
        
        dataset.setHeader(header.toArray(new String[0]));
    }
}
