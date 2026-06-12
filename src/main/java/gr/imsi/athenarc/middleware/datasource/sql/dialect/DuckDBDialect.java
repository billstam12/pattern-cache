package gr.imsi.athenarc.middleware.datasource.sql.dialect;

/**
 * DuckDB SQL dialect. Time functions are Postgres-compatible (epoch / to_timestamp);
 * representative-of-group uses DuckDB's native {@code any_value()}.
 */
public class DuckDBDialect implements SqlDialect {

    @Override
    public boolean supportsSlopeAggregates() {
        return true;
    }

    @Override
    public String unixSecondsToTimestamp(String unixSecondsExpr) {
        // make_timestamp(BIGINT microseconds) returns plain TIMESTAMP (UTC, no TZ),
        // matching the type produced by read_csv_auto on un-zoned timestamp strings.
        // Avoids the TIMESTAMPTZ vs TIMESTAMP cast issue that to_timestamp would introduce.
        return "make_timestamp(CAST((" + unixSecondsExpr + ") * 1000000 AS BIGINT))";
    }

    @Override
    public String timestampToUnixMillis(String timestampExpr) {
        return "epoch_ms(" + timestampExpr + ")";
    }

    @Override
    public String timestampToUnixSeconds(String timestampExpr) {
        return "epoch(" + timestampExpr + ")";
    }

    @Override
    public String anyValue(String columnExpr) {
        return "any_value(" + columnExpr + ")";
    }

    @Override
    public String firstOfArrayAgg(String elementExpr, String orderByExpr, boolean descending) {
        String direction = descending ? " DESC" : "";
        return "list_sort(array_agg(" + elementExpr + " ORDER BY " + orderByExpr + direction + "))[1]";
    }
}
