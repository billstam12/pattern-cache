package gr.imsi.athenarc.visual.middleware.datasource.connection;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

public interface DatabaseConnection {

    public DatabaseConnection connect() throws  SQLException;
    
    public QueryExecutor getQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
    
}