package gr.imsi.athenarc.middleware.datasource.dataset;
import java.util.*;
import java.util.stream.Collectors;

import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;


public abstract class AbstractDataset {

    // yyyy-MM-dd HH:mm:ss.SSS
    private static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";

    private String id;

    private String[] header;
    private String schema;
    private String tableName;
    private String timeFormat;
    private TimeRange timeRange;

    private long samplingInterval;

    public AbstractDataset(){}

    public AbstractDataset(String id, String schema, String tableName){
        this(id, schema, tableName, DEFAULT_FORMAT);
    }

    // Constructor with time format
    public AbstractDataset(String id, String schema, String tableName, String timeFormat){
        this.id = id;
        this.schema = schema;
        this.tableName = tableName;
        this.timeFormat = timeFormat;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TimeInterval getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public String[] getHeader() {
        String[] sortedHeader = Arrays.copyOf(header, header.length);
        Arrays.sort(sortedHeader);
        return sortedHeader;
    }

    public void setHeader(String[] header) { this.header = header; }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public void setSamplingInterval(long samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractDataset)) {
            return false;
        }
        return id != null && id.equals(((AbstractDataset) o).id);
    }
}

