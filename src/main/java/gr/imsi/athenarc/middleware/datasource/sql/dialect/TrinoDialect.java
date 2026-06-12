package gr.imsi.athenarc.middleware.datasource.sql.dialect;

public class TrinoDialect implements SqlDialect {

    @Override
    public boolean supportsSlopeAggregates() {
        return true;
    }

    @Override
    public String unixSecondsToTimestamp(String unixSecondsExpr) {
        return "from_unixtime(" + unixSecondsExpr + ")";
    }

    @Override
    public String timestampToUnixMillis(String timestampExpr) {
        return "to_unixtime(" + timestampExpr + ") * 1000";
    }

    @Override
    public String timestampToUnixSeconds(String timestampExpr) {
        return "to_unixtime(" + timestampExpr + ")";
    }

    @Override
    public String anyValue(String columnExpr) {
        return "arbitrary(" + columnExpr + ")";
    }

    @Override
    public String firstOfArrayAgg(String elementExpr, String orderByExpr, boolean descending) {
        String direction = descending ? " DESC" : "";
        return "element_at(array_sort(array_agg(" + elementExpr + " ORDER BY " + orderByExpr + direction + ")), 1)";
    }
}
