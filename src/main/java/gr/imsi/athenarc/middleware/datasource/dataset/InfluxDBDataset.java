package gr.imsi.athenarc.middleware.datasource.dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBDataset extends AbstractDataset {

    private static String DEFAULT_INFLUX_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDataset.class);


    public InfluxDBDataset(){}
    
    // Abstract class implementation
    public InfluxDBDataset(String id, String bucket, String measurement){
        super(id, bucket, measurement, DEFAULT_INFLUX_FORMAT);
    }
    
    @Override
    public String toString() {
        return "InfluxDBDataset{" +
                ", bucket='" + getSchema() + '\'' +
                ", measurement='" + getTableName() + '\'' +
                '}';
    }
}
