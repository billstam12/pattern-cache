package gr.imsi.athenarc.visual.middleware.datasource.executor;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface QueryExecutor {

    Map<Integer, List<DataPoint>> execute(String query) throws IOException, SQLException;
    Map<Integer, List<DataPoint>> executeM4Query(DataSourceQuery q) throws IOException, SQLException;
    Map<Integer, List<DataPoint>> executeRawQuery(DataSourceQuery q) throws IOException, SQLException;
    Map<Integer, List<DataPoint>> executeMinMaxQuery(DataSourceQuery q) throws SQLException, IOException;
}
