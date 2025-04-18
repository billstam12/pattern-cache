package gr.imsi.athenarc.middleware.datasource.connection;

import java.sql.SQLException;


public interface DatabaseConnection {

    public DatabaseConnection connect() throws  SQLException;
    
    public void closeConnection() throws SQLException;
    
}