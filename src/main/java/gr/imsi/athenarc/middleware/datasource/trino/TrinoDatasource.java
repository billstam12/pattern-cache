package gr.imsi.athenarc.middleware.datasource.trino;

import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.datasource.sql.SqlLikeDatasource;
import gr.imsi.athenarc.middleware.datasource.sql.dialect.TrinoDialect;

public class TrinoDatasource extends SqlLikeDatasource {

    public TrinoDatasource(SQLQueryExecutor trinoQueryExecutor, SQLDataset dataset) {
        super(trinoQueryExecutor, dataset, new TrinoDialect());
    }
}
