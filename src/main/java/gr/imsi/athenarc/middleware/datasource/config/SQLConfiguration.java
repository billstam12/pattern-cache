package gr.imsi.athenarc.middleware.datasource.config;

public class SQLConfiguration implements DataSourceConfiguration {
    
    private String url;
    private String username;
    private String password;
    private String schemaName;
    private String tableName;
    private String timestampColumn;
    private String idColumn;
    private String valueColumn;
    private String timeFormat;

    public SQLConfiguration(String url, String username, String password, String schemaName, String tableName,
                           String timestampColumn, String idColumn, String valueColumn, String timeFormat) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.schemaName = schemaName;   
        this.tableName = tableName;
        this.timestampColumn = timestampColumn;
        this.idColumn = idColumn;
        this.valueColumn = valueColumn;
        this.timeFormat = timeFormat;
    }

    private SQLConfiguration() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String tableName;
        private String timestampColumn;
        private String idColumn;
        private String valueColumn;
        private String schemaName;
        private String timeFormat;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder timestampColumn(String timestampColumn) {
            this.timestampColumn = timestampColumn;
            return this;
        }

        public Builder idColumn(String idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        public Builder valueColumn(String valueColumn) {
            this.valueColumn = valueColumn;
            return this;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder timeFormat(String timeFormat) {
            this.timeFormat = timeFormat;
            return this;
        }

        public SQLConfiguration build() {
            SQLConfiguration config = new SQLConfiguration();
            config.url = this.url;
            config.username = this.username;
            config.password = this.password;
            config.tableName = this.tableName;
            config.timestampColumn = this.timestampColumn;
            config.idColumn = this.idColumn;
            config.valueColumn = this.valueColumn;
            config.schemaName = this.schemaName;
            config.timeFormat = this.timeFormat;
            return config;
        }
    }

    public String getUrl() { return url; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getTableName() { return tableName; }
    public String getTimestampColumn() { return timestampColumn; }
    public String getIdColumn() { return idColumn; }
    public String getValueColumn() { return valueColumn; }
    public String getSchemaName() { return schemaName; }
    public String getTimeFormat() { return timeFormat; }

    public void setUrl(String url) { this.url = url; }
    public void setUsername(String username) { this.username = username; }
    public void setPassword(String password) { this.password = password; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public void setTimestampColumn(String timestampColumn) { this.timestampColumn = timestampColumn; }
    public void setIdColumn(String idColumn) { this.idColumn = idColumn; }
    public void setValueColumn(String valueColumn) { this.valueColumn = valueColumn; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public void setTimeFormat(String timeFormat) { this.timeFormat = timeFormat; }

}
