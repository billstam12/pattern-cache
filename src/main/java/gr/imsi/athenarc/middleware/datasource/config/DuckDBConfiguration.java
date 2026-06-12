package gr.imsi.athenarc.middleware.datasource.config;

/**
 * DuckDB configuration. The {@code url} is a JDBC URL like
 * {@code jdbc:duckdb:} for in-memory or {@code jdbc:duckdb:/path/to/foo.duckdb} for persistent.
 *
 * If {@code csvPath} is set, the data source factory exposes the CSV under the configured table:
 * when {@code cacheDb} is true the CSV is materialized into a persistent table; when false an
 * in-memory view is created so every query re-reads the CSV file.
 */
public class DuckDBConfiguration implements DataSourceConfiguration {

    private String url;
    private String csvPath;
    private String schemaName;
    private String tableName;
    private String timestampColumn;
    private String idColumn;
    private String valueColumn;
    private String timeFormat;
    private boolean cacheDb;

    private DuckDBConfiguration() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private String csvPath;
        private String schemaName;
        private String tableName;
        private String timestampColumn;
        private String idColumn;
        private String valueColumn;
        private String timeFormat;
        private boolean cacheDb;

        public Builder url(String url) { this.url = url; return this; }
        public Builder csvPath(String csvPath) { this.csvPath = csvPath; return this; }
        public Builder schemaName(String schemaName) { this.schemaName = schemaName; return this; }
        public Builder tableName(String tableName) { this.tableName = tableName; return this; }
        public Builder timestampColumn(String timestampColumn) { this.timestampColumn = timestampColumn; return this; }
        public Builder idColumn(String idColumn) { this.idColumn = idColumn; return this; }
        public Builder valueColumn(String valueColumn) { this.valueColumn = valueColumn; return this; }
        public Builder timeFormat(String timeFormat) { this.timeFormat = timeFormat; return this; }
        public Builder cacheDb(boolean cacheDb) { this.cacheDb = cacheDb; return this; }

        public DuckDBConfiguration build() {
            DuckDBConfiguration cfg = new DuckDBConfiguration();
            cfg.url = url == null ? "jdbc:duckdb:" : url;
            cfg.csvPath = csvPath;
            cfg.schemaName = schemaName;
            cfg.tableName = tableName;
            cfg.timestampColumn = timestampColumn;
            cfg.idColumn = idColumn;
            cfg.valueColumn = valueColumn;
            cfg.timeFormat = timeFormat;
            cfg.cacheDb = cacheDb;
            return cfg;
        }
    }

    public String getUrl() { return url; }
    public String getCsvPath() { return csvPath; }
    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public String getTimestampColumn() { return timestampColumn; }
    public String getIdColumn() { return idColumn; }
    public String getValueColumn() { return valueColumn; }
    public String getTimeFormat() { return timeFormat; }
    public boolean isCacheDb() { return cacheDb; }
}
