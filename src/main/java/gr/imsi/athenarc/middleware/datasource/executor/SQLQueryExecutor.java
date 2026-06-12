package gr.imsi.athenarc.middleware.datasource.executor;

import gr.imsi.athenarc.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.middleware.datasource.connection.JDBCConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);

    private final JDBCConnection databaseConnection;

    public SQLQueryExecutor(DatabaseConnection databaseConnection) {
        this.databaseConnection = (JDBCConnection) databaseConnection;
    }

    public ResultSet executeDbQuery(String query) {
        LOG.debug("Executing Query: \n" + query);
        try {
            return databaseConnection.getConnection().createStatement().executeQuery(query);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing query: " + query, e);
        }
    }

    /** Run a DDL or DML statement that does not return a result set. */
    public void executeStatement(String statement) {
        LOG.debug("Executing statement: \n" + statement);
        try (java.sql.Statement st = databaseConnection.getConnection().createStatement()) {
            st.execute(statement);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing statement: " + statement, e);
        }
    }

    public void closeConnection() {
        try {
            databaseConnection.closeConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Error closing connection", e);
        };
    }

    public JDBCConnection getJdbcConnection() {
        return databaseConnection;
    }

}