package gr.imsi.athenarc.middleware.datasource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.config.*;
import gr.imsi.athenarc.middleware.datasource.connection.*;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.datasource.executor.*;
import gr.imsi.athenarc.middleware.datasource.influx.InfluxDBDatasource;
import gr.imsi.athenarc.middleware.datasource.sql.SQLDatasource;
import gr.imsi.athenarc.middleware.datasource.trino.TrinoDatasource;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeRange;

public class DataSourceFactory {

    public static final Logger LOG = LoggerFactory.getLogger(DataSourceFactory.class);
    public static DataSource createDataSource(DataSourceConfiguration config) {
        if (config instanceof InfluxDBConfiguration) {
            return createInfluxDBDataSource((InfluxDBConfiguration) config);
        } else if (config instanceof SQLConfiguration) {
            return createSQLDataSource((SQLConfiguration) config);
        } else if (config instanceof TrinoConfiguration) {
            return createTrinoDataSource((TrinoConfiguration) config);
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

        InfluxDBDataset dataset;
        
        // Check if dataset info exists in cache
        if (DatasetCache.hasDataset("influxdb", config.getBucket(), config.getMeasurement())) {
            dataset = (InfluxDBDataset) DatasetCache.getDataset("influxdb", config.getBucket(), config.getMeasurement());
        } else {
            dataset = new InfluxDBDataset(
                config.getMeasurement(),
                config.getBucket(),
                config.getMeasurement()
            );
            fillInfluxDBDatasetInfo(dataset, executor);
            
            // Save dataset info to cache
            DatasetCache.saveDataset("influxdb", dataset);
        }

        return new InfluxDBDatasource(executor, dataset);
    }

    private static DataSource createSQLDataSource(SQLConfiguration config) {
        JDBCConnection jdbcConnection = new JDBCConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        jdbcConnection.connect();
        SQLQueryExecutor executor = new SQLQueryExecutor(jdbcConnection);

        SQLDataset dataset;
        
        // Check if dataset info exists in cache
        if (DatasetCache.hasDataset("sql", config.getSchemaName(), config.getTableName())) {
            dataset = (SQLDataset) DatasetCache.getDataset("sql", config.getSchemaName(), config.getTableName());
            dataset.setTimeFormat(config.getTimeFormat());
            dataset.setIdColumn(config.getIdColumn());
            dataset.setValueColumn(config.getValueColumn());
            dataset.setTimestampColumn(config.getTimestampColumn());
        } else {
            dataset = new SQLDataset(
                config.getSchemaName(),
                config.getTableName(),
                config.getTimestampColumn(),
                config.getIdColumn(),
                config.getValueColumn(),
                config.getTimeFormat()
            );
            fillSQLDatasetInfo(dataset, executor);
            
            // Save dataset info to cache
            DatasetCache.saveDataset("sql", dataset);
        }
        LOG.info("Created SQL dataset: {}", dataset);
        return new SQLDatasource(executor, dataset);
    }

    private static DataSource createTrinoDataSource(TrinoConfiguration config) {
        JDBCConnection jdbcConnection = new JDBCConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        jdbcConnection.connect();
        SQLQueryExecutor executor = new SQLQueryExecutor(jdbcConnection);

        SQLDataset dataset;
        
        // Check if dataset info exists in cache
        if (DatasetCache.hasDataset("trino", config.getSchemaName(), config.getTableName())) {
            dataset = (SQLDataset) DatasetCache.getDataset("trino", config.getSchemaName(), config.getTableName());
            dataset.setTimeFormat(config.getTimeFormat());
            dataset.setIdColumn(config.getIdColumn());
            dataset.setValueColumn(config.getValueColumn());
            dataset.setTimestampColumn(config.getTimestampColumn());
        } else {
            dataset = new SQLDataset(
                config.getSchemaName(),
                config.getTableName(),
                config.getTimestampColumn(),
                config.getIdColumn(),
                config.getValueColumn(),
                config.getTimeFormat()
            );
            fillTrinoDatasetInfo(dataset, executor);
            
            // Save dataset info to cache
            DatasetCache.saveDataset("trino", dataset);
        }
        LOG.info("Created Trino dataset: {}", dataset);
        return new TrinoDatasource(executor, dataset);
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
        LOG.debug("InfluxDB first timestamp: {} (raw: {})", firstTime, firstRecord.getTime());
    
        // Fetch last timestamp
        String lastQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> last()\n";
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(lastQuery);
        FluxRecord lastRecord = fluxTables.get(0).getRecords().get(0);
        long lastTime = lastRecord.getTime().toEpochMilli();
        LOG.debug("InfluxDB last timestamp: {} (raw: {})", lastTime, lastRecord.getTime());

        String influxFormat = "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"";
        // Fetch the second timestamp to calculate the sampling interval.
        String secondQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start:" + DateTimeUtil.format(firstTime, influxFormat).replace("\"", "") + 
            ", stop: " + DateTimeUtil.format(firstTime + 360000, influxFormat).replace("\"", "") + ")\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> limit(n: 2)\n";
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(secondQuery);
        FluxRecord secondRecord = fluxTables.get(0).getRecords().get(1); 
        long secondTime = secondRecord.getTime().toEpochMilli();
        LOG.debug("InfluxDB second timestamp: {} (raw: {})", secondTime, secondRecord.getTime());
    
        // Calculate and set sampling interval
        long samplingInterval = secondTime - firstTime;
        LOG.debug("InfluxDB calculated sampling interval: {} ms", samplingInterval);
        dataset.setSamplingInterval(samplingInterval);
    
        // Set time range and headers
        dataset.setTimeRange(new TimeRange(firstTime, lastTime));
    
        // Populate header (field keys)
        Set<String> header = fluxTables.stream()
            .flatMap(fluxTable -> fluxTable.getRecords().stream())
            .map(FluxRecord::getField)
            .collect(Collectors.toSet());
        
        dataset.setHeader(header.toArray(new String[0]));
    }

    private static void fillSQLDatasetInfo(SQLDataset dataset, SQLQueryExecutor sqlQueryExecutor) {
        try {
            // Fetch first timestamp
            String firstQuery = "SELECT MIN(" + dataset.getTimestampColumn() + ") as first_time FROM " + dataset.getTableName();
            ResultSet firstResult = sqlQueryExecutor.executeDbQuery(firstQuery);
            firstResult.next();
            long firstTime = firstResult.getTimestamp("first_time").toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            firstResult.close();

            // Fetch last timestamp
            String lastQuery = "SELECT MAX(" + dataset.getTimestampColumn() + ") as last_time FROM " + dataset.getTableName();
            ResultSet lastResult = sqlQueryExecutor.executeDbQuery(lastQuery);
            lastResult.next();
            long lastTime = lastResult.getTimestamp("last_time").toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            lastResult.close();

            // Fetch the second timestamp to calculate the sampling interval
            String secondQuery = "SELECT " + dataset.getTimestampColumn() + " FROM " + dataset.getTableName() + 
                               " ORDER BY " + dataset.getIdColumn() + ", " + dataset.getTimestampColumn() + " ASC LIMIT 2";

            ResultSet secondResult = sqlQueryExecutor.executeDbQuery(secondQuery);
            secondResult.next();
            long samplingInterval = 0;
            if (secondResult.next()) {
                long secondTime = secondResult.getTimestamp(dataset.getTimestampColumn()).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
                samplingInterval = secondTime - firstTime;
            }
            secondResult.close();

            // Set sampling interval and time range
            dataset.setSamplingInterval(samplingInterval);
            dataset.setTimeRange(new TimeRange(firstTime, lastTime));

            // Populate header (distinct sensor/measure IDs)
            String headerQuery = "SELECT DISTINCT " + dataset.getIdColumn() + " FROM " + dataset.getTableName();
            ResultSet headerResult = sqlQueryExecutor.executeDbQuery(headerQuery);
            List<String> headers = new ArrayList<>();
            while (headerResult.next()) {
                headers.add(headerResult.getString(dataset.getIdColumn()));
            }
            headerResult.close();
            
            dataset.setHeader(headers.toArray(new String[0]));

        } catch (SQLException e) {
            throw new RuntimeException("Failed to fill SQL dataset info", e);
        }
    }

    private static void fillTrinoDatasetInfo(SQLDataset dataset, SQLQueryExecutor trinoQueryExecutor) {
        try {
            // Fetch first timestamp using Trino-specific functions
            String firstQuery = "SELECT min(" + dataset.getTimestampColumn() + ") as first_time FROM " + dataset.getTableName();
            ResultSet firstResult = trinoQueryExecutor.executeDbQuery(firstQuery);
            firstResult.next();
            long firstTime = firstResult.getTimestamp("first_time").toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            firstResult.close();

            // Fetch last timestamp using Trino-specific functions
            String lastQuery = "SELECT max(" + dataset.getTimestampColumn() + ") as last_time FROM " + dataset.getTableName();
            ResultSet lastResult = trinoQueryExecutor.executeDbQuery(lastQuery);
            lastResult.next();
            long lastTime = lastResult.getTimestamp("last_time").toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            lastResult.close();

            // Fetch the second timestamp to calculate the sampling interval
            String secondQuery = "SELECT " + dataset.getTimestampColumn() + " FROM " + dataset.getTableName() + 
                               " ORDER BY " + dataset.getIdColumn() + ", " + dataset.getTimestampColumn() + " ASC LIMIT 2";

            ResultSet secondResult = trinoQueryExecutor.executeDbQuery(secondQuery);
            secondResult.next();
            long samplingInterval = 0;
            if (secondResult.next()) {
                long secondTime = secondResult.getTimestamp(dataset.getTimestampColumn()).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
                samplingInterval = secondTime - firstTime;
            }
            secondResult.close();

            // Set sampling interval and time range
            dataset.setSamplingInterval(samplingInterval);
            dataset.setTimeRange(new TimeRange(firstTime, lastTime));

            // Populate header (distinct sensor/measure IDs)
            String headerQuery = "SELECT DISTINCT " + dataset.getIdColumn() + " FROM " + dataset.getTableName();
            ResultSet headerResult = trinoQueryExecutor.executeDbQuery(headerQuery);
            List<String> headers = new ArrayList<>();
            while (headerResult.next()) {
                headers.add(headerResult.getString(dataset.getIdColumn()));
            }
            headerResult.close();
            
            dataset.setHeader(headers.toArray(new String[0]));

        } catch (SQLException e) {
            throw new RuntimeException("Failed to fill Trino dataset info", e);
        }
    }
}
