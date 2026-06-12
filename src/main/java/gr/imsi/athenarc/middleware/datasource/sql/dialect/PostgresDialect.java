package gr.imsi.athenarc.middleware.datasource.sql.dialect;

public class PostgresDialect implements SqlDialect {

    @Override
    public boolean supportsSlopeAggregates() {
        return true;
    }

    @Override
    public String unixSecondsToTimestamp(String unixSecondsExpr) {
        // to_timestamp() returns TIMESTAMPTZ; AT TIME ZONE 'UTC' strips the zone and
        // yields a naive TIMESTAMP whose wall-clock fields equal the UTC time. This
        // matches the data column's TIMESTAMP-without-tz type and lets
        // DateTimeUtil.toEpochMilliUtc read it back without a JVM-TZ shift.
        return "(to_timestamp(" + unixSecondsExpr + ") AT TIME ZONE 'UTC')";
    }

    @Override
    public String timestampToUnixMillis(String timestampExpr) {
        return "EXTRACT(EPOCH FROM " + timestampExpr + ") * 1000";
    }

    @Override
    public String timestampToUnixSeconds(String timestampExpr) {
        return "EXTRACT(EPOCH FROM " + timestampExpr + ")";
    }

    @Override
    public String anyValue(String columnExpr) {
        return "MIN(" + columnExpr + ")";
    }

    @Override
    public String firstOfArrayAgg(String elementExpr, String orderByExpr, boolean descending) {
        String direction = descending ? " DESC" : "";
        return "(ARRAY_AGG(" + elementExpr + " ORDER BY " + orderByExpr + direction + "))[1]";
    }
}
