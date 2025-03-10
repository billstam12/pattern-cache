package gr.imsi.athenarc.visual.middleware;

import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.visual.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.patterncache.PatternCache;
import gr.imsi.athenarc.visual.middleware.patterncache.query.PatternQuery;
import gr.imsi.athenarc.visual.middleware.patterncache.query.SegmentSpecification;
import gr.imsi.athenarc.visual.middleware.patterncache.query.TimeFilter;
import gr.imsi.athenarc.visual.middleware.patterncache.query.ValueFilter;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Properties properties = readProperties();

        // Connetion properties
        String influxUrl = properties.getProperty("influxdb.url");
        String org = properties.getProperty("influxdb.org");
        String token = properties.getProperty("influxdb.token");  
    
        // Dataset properties
        String bucket = "more";
        String measurement = "spx";
        String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";
        InfluxDBConfiguration influxDBConfiguration = new InfluxDBConfiguration.Builder()
            .url(influxUrl)
            .org(org)
            .token(token)
            .bucket(bucket)
            .timeFormat(timeFormat)
            .measurement(measurement)
            .build();   
            
        DataSource influxDataSource = DataSourceFactory.createDataSource(influxDBConfiguration);

        // Query properties
        long from = DateTimeUtil.parseDateTimeString("2006-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        long to = DateTimeUtil.parseDateTimeString("2011-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        int measure = 1;
        ChronoUnit chronoUnit = ChronoUnit.DAYS;
        List<SegmentSpecification> segmentSpecs = new ArrayList<>();
        TimeFilter singleUnitTimeFilter = new TimeFilter(false, 1, 1);
        
        ValueFilter smallSlopeUpValueFilter = new ValueFilter(false, 0.05, 0.1);
        ValueFilter smallSlopeDownValueFilter = new ValueFilter(false, -0.1, -0.05);
        ValueFilter largeSlopeDownValueFilter = new ValueFilter(false, -1, -0.2);
        
        SegmentSpecification upSpec = new SegmentSpecification(singleUnitTimeFilter, smallSlopeUpValueFilter);
        SegmentSpecification downSpec = new SegmentSpecification(singleUnitTimeFilter, smallSlopeDownValueFilter);
        SegmentSpecification largeDownSpec = new SegmentSpecification(singleUnitTimeFilter, largeSlopeDownValueFilter);

        segmentSpecs.add(upSpec);
        segmentSpecs.add(downSpec);
        segmentSpecs.add(upSpec);
        segmentSpecs.add(downSpec);
        segmentSpecs.add(upSpec);
        segmentSpecs.add(largeDownSpec);

        PatternQuery patternQuery = new PatternQuery(from, to, measure, chronoUnit, segmentSpecs);
        PatternCache patternCache = new PatternCache(influxDataSource);

        patternCache.executeQuery(patternQuery);

        influxDataSource.closeConnection();
    }

    public static Properties readProperties(){
        Properties properties = new Properties();
        // Load from the resources folder
        // "/application.properties" assumes the file is at src/main/resources/application.properties
        try (InputStream input = Main.class.getResourceAsStream("/application.properties")) {
            if (input == null) {
                LOG.error("Sorry, unable to find application.properties in resources.");
                return null;
            }
            properties.load(input);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return properties;
    }
}