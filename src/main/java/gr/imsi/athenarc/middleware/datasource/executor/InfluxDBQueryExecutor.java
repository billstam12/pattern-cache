package gr.imsi.athenarc.middleware.datasource.executor;

import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.middleware.datasource.connection.InfluxDBConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBQueryExecutor.class);

    private final InfluxDBConnection databaseConnection;

    public InfluxDBQueryExecutor(DatabaseConnection databaseConnection) {
        this.databaseConnection = (InfluxDBConnection) databaseConnection;
    }

    public List<FluxTable> executeDbQuery(String query) {
        QueryApi queryApi = databaseConnection.getClient().getQueryApi();
        LOG.info("Executing Query: \n" + query);
        return queryApi.query(query);
    }

    public void closeConnection() {
        databaseConnection.closeConnection();;
    }

}