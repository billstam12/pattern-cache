package gr.imsi.athenarc.middleware.datasource.dataset;

public class SQLDataset extends AbstractDataset {
    
    private String timestampColumn;
    private String idColumn;
    private String valueColumn;


    // Abstract class implementation

    public SQLDataset(String id, String bucket, String measurement){
        super(id, bucket, measurement);
    }
    public SQLDataset(String schemaName, String tableName, String timestampColumn, String idColumn, String valueColumn, String timeFormat) {
        super(tableName, schemaName, tableName, timeFormat);
        this.timestampColumn = timestampColumn;
        this.idColumn = idColumn;
        this.valueColumn = valueColumn;
    }

    public String getTimestampColumn() {
        return timestampColumn;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public String getValueColumn() {
        return valueColumn;
    }

    public void setTimestampColumn(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public void setValueColumn(String valueColumn) {
        this.valueColumn = valueColumn;
    }

    public String toString() {
        return "SQLDataset{" +
                "id='" + getId() + '\'' +
                ", schemaName='" + getSchema() + '\'' +
                ", tableName='" + getTableName() + '\'' +
                ", timestampColumn='" + timestampColumn + '\'' +
                ", idColumn='" + idColumn + '\'' +
                ", valueColumn='" + valueColumn + '\'' +
                ", timeFormat='" + getTimeFormat() + '\'' +
                ", samplingInterval=" + getSamplingInterval() +
                ", timeRange=" + getTimeRange() +
                ", header=" + String.join(", ", getHeader()) +
                '}';
    }
}
