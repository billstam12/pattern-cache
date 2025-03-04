package gr.imsi.athenarc.visual.middleware.datasource.config;

public class PostgeSQLConfiguration implements DataSourceConfiguration {
    private final String url;
    private final String username;
    private final String password;
    private final String schema;
    private final String id;
    private final String timeFormat;

    private PostgeSQLConfiguration(Builder builder) {
        this.url = builder.url;
        this.username = builder.username;
        this.password = builder.password;
        this.schema = builder.schema;
        this.id = builder.id;
        this.timeFormat = builder.timeFormat;
    }

    public String getUrl() { return url; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    @Override public String getSchema() { return schema; }
    @Override public String getId() { return id; }
    @Override public String getTimeFormat() { return timeFormat; }

    public static class Builder {
        private String url, username, password, schema, id, timeFormat;

        public Builder url(String url) { this.url = url; return this; }
        public Builder username(String username) { this.username = username; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder schema(String schema) { this.schema = schema; return this; }
        public Builder id(String id) { this.id = id; return this; }
        public Builder timeFormat(String timeFormat) { this.timeFormat = timeFormat; return this; }

        public PostgeSQLConfiguration build() {
            return new PostgeSQLConfiguration(this);
        }
    }
}
