package gr.imsi.athenarc.middleware.datasource.sql;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.PostgresDialect;

public class SQLDatasource extends SqlLikeDatasource {

    public SQLDatasource(SQLQueryExecutor sqlQueryExecutor, SQLDataset dataset) {
        super(sqlQueryExecutor, dataset, new PostgresDialect());
    }
}
