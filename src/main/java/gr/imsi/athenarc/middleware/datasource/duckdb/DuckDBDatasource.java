package gr.imsi.athenarc.middleware.datasource.duckdb;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.sql.SqlLikeDatasource;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.DuckDBDialect;

public class DuckDBDatasource extends SqlLikeDatasource {

    public DuckDBDatasource(SQLQueryExecutor queryExecutor, SQLDataset dataset) {
        super(queryExecutor, dataset, new DuckDBDialect());
    }
}
