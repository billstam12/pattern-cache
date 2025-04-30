package gr.imsi.athenarc.middleware;

import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.initialization.MemoryBoundedInitializationPolicy;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregationType;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.manager.CacheManager;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

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
        long from = DateTimeUtil.parseDateTimeString("2006-01-01 00:14:39", "yyyy-MM-dd HH:mm:ss");
        long to = DateTimeUtil.parseDateTimeString("2011-01-01 00:01:24", "yyyy-MM-dd HH:mm:ss");
        int measure = 1;
        ChronoUnit chronoUnit = ChronoUnit.DAYS;
        List<PatternNode> segmentSpecs = new ArrayList<>();
        TimeFilter singleUnitTimeFilter = new TimeFilter(false, 1, 2);
        
        ValueFilter smallSlopeUpValueFilter = new ValueFilter(false, 0.01, 0.1);
        ValueFilter smallSlopeDownValueFilter = new ValueFilter(false, -0.1, -0.01);
        ValueFilter largeSlopeDownValueFilter = new ValueFilter(false, -0.5, -0.2);
        ValueFilter largeSlopeUpValueFilter = new ValueFilter(false, 0.2, 0.5);

        SegmentSpecification upSpec = new SegmentSpecification(singleUnitTimeFilter, smallSlopeUpValueFilter);
        SegmentSpecification downSpec = new SegmentSpecification(singleUnitTimeFilter, smallSlopeDownValueFilter);
        SegmentSpecification largeDownSpec = new SegmentSpecification(singleUnitTimeFilter, largeSlopeDownValueFilter);
        SegmentSpecification largeUpSpec = new SegmentSpecification(singleUnitTimeFilter, largeSlopeUpValueFilter);

        SingleNode upNode = new SingleNode(upSpec, RepetitionFactor.exactly(1));
        SingleNode downNode = new SingleNode(downSpec,  RepetitionFactor.exactly(1));
        SingleNode largeDownNode = new SingleNode(largeDownSpec,  RepetitionFactor.exactly(1));
        SingleNode largeUpNode = new SingleNode(largeUpSpec,  RepetitionFactor.exactly(1));

        List<PatternNode> upDownNode = new ArrayList<>();

        upDownNode.add(largeUpNode);
        upDownNode.add(largeDownNode);
        GroupNode groupNode0 = new GroupNode(upDownNode, RepetitionFactor.exactly(1));

        segmentSpecs.add(groupNode0);
        // segmentSpecs.add(largeDownNode);

        AggregateInterval timeUnit = AggregateInterval.of(1, chronoUnit);
        AggregationType aggregationType = AggregationType.LAST_VALUE;
        PatternQuery patternQuery = new PatternQuery(from, to, measure, timeUnit,aggregationType, segmentSpecs);
        CacheManager queryManager = CacheManager.createDefault(influxDataSource);
        queryManager.initializeCache(new MemoryBoundedInitializationPolicy(
            512 * 1024 * 1024, // 512MB memory limit
            0.1));
        
        queryManager.executeQuery(patternQuery);
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