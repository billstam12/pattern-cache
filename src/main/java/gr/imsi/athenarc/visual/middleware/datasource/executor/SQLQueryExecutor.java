package gr.imsi.athenarc.visual.middleware.datasource.executor;

import gr.imsi.athenarc.visual.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connection.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

import java.util.*;

public class SQLQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);

    PostgreSQLDataset dataset;
    JDBCConnection connection;
    private final String dropFolder = "postgres-drop-queries";
    private final String initFolder = "postgres-init-queries";

    public SQLQueryExecutor(DatabaseConnection connection, AbstractDataset dataset) {
        this.connection = (JDBCConnection) connection;
        this.dataset = (PostgreSQLDataset) dataset;
    }

    @Override
    public Map<Integer, List<DataPoint>> executeM4Query(DataSourceQuery q) throws SQLException {
        return collectData(executeM4SqlQuery((SQLQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeRawQuery(DataSourceQuery q) throws SQLException {
        return collectData(executeRawSqlQuery((SQLQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeMinMaxQuery(DataSourceQuery q) throws SQLException {
        return collectData(executeMinMaxSqlQuery((SQLQuery) q));
    }

    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1==null && s2==null) return 0; //swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };


    public ResultSet executeRawSqlQuery(SQLQuery q) throws SQLException{
        String query = q.rawQuerySkeleton();
        return executeDbQuery(query);
    }


    public ResultSet executeM4SqlQuery(SQLQuery q) throws SQLException {
        String query = q.m4QuerySkeleton();
        return executeDbQuery(query);
    }


    public ResultSet executeMinMaxSqlQuery(SQLQuery q) throws SQLException {
        String query = q.minMaxQuerySkeleton();
        return executeDbQuery(query);
    }


    private Map<Integer, List<DataPoint>> collectData(ResultSet resultSet) throws SQLException {
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        while(resultSet.next()){
            Integer measure = Arrays.asList(dataset.getHeader()).indexOf(resultSet.getString(1)); // measure
            long epoch = resultSet.getLong(2); // min_timestamp
            long epoch2 = resultSet.getLong(3); // max_timestamp
            Double val = resultSet.getObject(4) == null ? null : resultSet.getDouble(4); // value
            if(val == null) continue;
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                    new ImmutableDataPoint(epoch, val, measure));
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                    new ImmutableDataPoint(epoch2, val, measure));
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        return data;
    }
    
    public Map<Integer, List<DataPoint>> execute(String query) throws SQLException {
        return collectData(executeDbQuery(query));
    }

    public ResultSet executeDbQuery(String query) throws SQLException {
        LOG.debug("Executing Query: \n" + query);
        PreparedStatement preparedStatement =  connection.getConnection().prepareStatement(query);
        return preparedStatement.executeQuery();
    }

}

