package gr.imsi.athenarc.visual.middleware.datasource.config;

public class InfluxDBConfiguration implements DataSourceConfiguration {
    private final String url;
    private final String org;
    private final String token;
    private final String bucket;
    private final String schema;
    private final String id;
    private final String timeFormat;

    private InfluxDBConfiguration(Builder builder) {
        this.url = builder.url;
        this.org = builder.org;
        this.token = builder.token;
        this.bucket = builder.bucket;
        this.schema = builder.schema;
        this.id = builder.id;
        this.timeFormat = builder.timeFormat;
    }

    public String getUrl() { return url; }
    public String getOrg() { return org; }
    public String getToken() { return token; }
    public String getBucket() { return bucket; }
    @Override public String getSchema() { return schema; }
    @Override public String getId() { return id; }
    @Override public String getTimeFormat() { return timeFormat; }

    public static class Builder {
        private String url, org, token, bucket, schema, id, timeFormat;

        public Builder url(String url) { this.url = url; return this; }
        public Builder org(String org) { this.org = org; return this; }
        public Builder token(String token) { this.token = token; return this; }
        public Builder bucket(String bucket) { this.bucket = bucket; return this; }
        public Builder schema(String schema) { this.schema = schema; return this; }
        public Builder id(String id) { this.id = id; return this; }
        public Builder timeFormat(String timeFormat) { this.timeFormat = timeFormat; return this; }

        public InfluxDBConfiguration build() {
            return new InfluxDBConfiguration(this);
        }
    }
}
