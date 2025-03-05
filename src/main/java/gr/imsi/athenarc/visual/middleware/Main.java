package gr.imsi.athenarc.visual.middleware;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.visual.middleware.datasource.config.InfluxDBConfiguration;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Properties properties = readProperties();

        // Connetion properties
        String influxUrl = properties.getProperty("influxdb.url");
        String org = properties.getProperty("influxdb.org");
        String token = properties.getProperty("influxdb.token");    

        long from = 1583408619000L;
        long to = 1683408619000L;
        List<Integer> measures = Arrays.asList(2);
        String bucket = "more";
        String measurement = "intel_lab_exp";
        String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

        InfluxDBConfiguration influxDBConfiguration = new InfluxDBConfiguration.Builder()
            .url(influxUrl)
            .org(org)
            .token(token)
            .bucket(bucket)
            .timeFormat(timeFormat)
            .measurement(measurement)
            .build();   
        DataSource dataSource = DataSourceFactory.createDataSource(influxDBConfiguration);
    
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