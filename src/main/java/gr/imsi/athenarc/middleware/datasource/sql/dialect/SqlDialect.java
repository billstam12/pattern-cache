package gr.imsi.athenarc.middleware.datasource.sql.dialect;

/**
 * Engine-specific SQL fragments for the data-source classes that issue SQL.
 * Captures the small differences between Postgres/Trino/DuckDB so that the
 * SQL builders themselves can stay engine-agnostic.
 */
public interface SqlDialect {

    /** Whether {@link #getSlopeAggregates} is supported. Postgres throws today; Trino and DuckDB return aggregates. */
    boolean supportsSlopeAggregates();

    /** Wrap a unix-seconds (possibly fractional) expression into a TIMESTAMP value. */
    String unixSecondsToTimestamp(String unixSecondsExpr);

    /** Convenience overload for a double literal. */
    default String unixSecondsToTimestamp(double unixSeconds) {
        return unixSecondsToTimestamp(Double.toString(unixSeconds));
    }

    /** Convert the given timestamp expression into unix-time milliseconds (numeric). */
    String timestampToUnixMillis(String timestampExpr);

    /** Convert the given timestamp expression into unix-time seconds (numeric). */
    String timestampToUnixSeconds(String timestampExpr);

    /**
     * "Pick any value within the group" — used when collapsing window-function output (constant per group)
     * into a row per group. Postgres has no native helper; we use {@code MIN(x)} which is equivalent
     * because the column is constant per group.
     */
    String anyValue(String columnExpr);

    /**
     * Return the timestamp paired with the min/max value within the group, by sorting an array_agg
     * of timestamps ordered by value (ascending for min, descending for max) and taking the first element.
     */
    String firstOfArrayAgg(String elementExpr, String orderByExpr, boolean descending);
}
