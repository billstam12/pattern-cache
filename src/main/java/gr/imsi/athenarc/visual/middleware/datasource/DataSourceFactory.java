package gr.imsi.athenarc.visual.middleware.datasource;

import java.io.IOException;

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
        InfluxDBConnection connection = new InfluxDBConnection(
            config.getUrl(), 
            config.getOrg(), 
            config.getToken(), 
            config.getBucket()
        );
        
        InfluxDBDataset dataset = new InfluxDBDataset(
            config.getSchema(),
            config.getId(),
            config.getTimeFormat()
        );
        
        InfluxDBQueryExecutor executor = (InfluxDBQueryExecutor) connection.connect().getQueryExecutor(dataset);
        return new InfluxDBDatasource(executor, dataset);
    }

    private static DataSource createPostgreSQLDataSource(PostgeSQLConfiguration config) {
        JDBCConnection connection = new JDBCConnection(
            config.getUrl(), 
            config.getUsername(), 
            config.getPassword()
        );
        
        PostgreSQLDataset dataset = new PostgreSQLDataset(
            config.getSchema(),
            config.getId(),
            config.getTimeFormat()
        );
        
        SQLQueryExecutor executor = (SQLQueryExecutor) connection.connect().getQueryExecutor(dataset);
        return new PostgreSQLDatasource(executor, dataset);
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
