package gr.imsi.athenarc.visual.middleware.datasource;

import java.io.IOException;
import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.config.*;
import gr.imsi.athenarc.visual.middleware.datasource.connection.*;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.*;
import gr.imsi.athenarc.visual.middleware.datasource.executor.*;

public class DataSourceFactory {
    
    public static DataSource createDataSource(DataSourceConfiguration config) {
        if (config instanceof InfluxDBConfiguration) {
            return createInfluxDBDataSource((InfluxDBConfiguration) config);
        }
        else if (config instanceof PostgeSQLConfiguration) {
            return createPostgreSQLDataSource((PostgeSQLConfiguration) config);
        } else if (config instanceof CsvConfiguration) {
            return createCsvDataSource((CsvConfiguration) config);
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
        
        InfluxDBDataset dataset = new InfluxDBDataset(
            connection,
            config.getMeasurement(),
            config.getBucket(),
            config.getMeasurement()
        );
        
        InfluxDBQueryExecutor executor = (InfluxDBQueryExecutor) connection.connect().getQueryExecutor(dataset);
        return new InfluxDBDatasource(executor, dataset);
    }

    private static DataSource createPostgreSQLDataSource(PostgeSQLConfiguration config) {
        
        
        PostgreSQLDataset dataset;
        try {
            JDBCConnection connection = (JDBCConnection) new JDBCConnection(
            config.getUrl(), 
            config.getUsername(), 
            config.getPassword()
            ).connect();

            dataset = new PostgreSQLDataset(
                connection,
                config.getTable(),
                config.getSchema(),
                config.getTable()
            );
            SQLQueryExecutor executor = (SQLQueryExecutor) connection.connect().getQueryExecutor(dataset);
            return new PostgreSQLDatasource(executor, dataset);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static DataSource createCsvDataSource(CsvConfiguration config) {
        try {
            CsvDataset dataset = new CsvDataset(config.getPath(), config.getTimeFormat(), config.getTimeCol(), config.getDelimiter(), config.getHasHeader());
            QueryExecutor executor = new CsvQueryExecutor(dataset);
            return new CsvDatasource((CsvQueryExecutor) executor, (CsvDataset) dataset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
