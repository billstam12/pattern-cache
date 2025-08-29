package gr.imsi.athenarc.middleware.datasource;


import gr.imsi.athenarc.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.middleware.datasource.dataset.SQLDataset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class DatasetCache {
    private static final String CACHE_DIRECTORY = "dataset_cache";
    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    
    static {
        // Create cache directory if it doesn't exist
        try {
            Path cacheDirPath = Paths.get(CACHE_DIRECTORY);
            if (!Files.exists(cacheDirPath)) {
                Files.createDirectories(cacheDirPath);
            }
        } catch (IOException e) {
            System.err.println("Failed to create cache directory: " + e.getMessage());
        }
    }
    
    public static boolean hasDataset(String source, String schema, String tableName) {
        File cacheFile = getCacheFile(source, schema, tableName);
        return cacheFile.exists();
    }
    
    public static AbstractDataset getDataset(String source, String schema, String tableName) {
        File cacheFile = getCacheFile(source, schema, tableName);
        if (!cacheFile.exists()) {
            return null;
        }
        
        try {
            Map<String, Object> datasetData = mapper.readValue(cacheFile, Map.class);
            
            if (source.equals("influxdb")) {
                InfluxDBDataset dataset = new InfluxDBDataset(tableName, schema, tableName);
                populateDatasetFromCache(dataset, datasetData);
                return dataset;
            }
            else if(source.equals("sql") || source.equals("trino")) {
                SQLDataset dataset = new SQLDataset(tableName, schema, tableName);
                dataset.setId((String) datasetData.get("id"));
                populateDatasetFromCache(dataset, datasetData);
                return dataset;
            }
        } catch (IOException e) {
            System.err.println("Failed to read dataset from cache: " + e.getMessage());
        }
        
        return null;
    }
    
    public static void saveDataset(String source, AbstractDataset dataset) {
        File cacheFile = getCacheFile(source, dataset.getSchema(), dataset.getTableName());
        
        try {
            Map<String, Object> datasetData = new HashMap<>();
            datasetData.put("schema", dataset.getSchema());
            datasetData.put("tableName", dataset.getTableName());
            datasetData.put("id", dataset.getId());
            datasetData.put("samplingInterval", dataset.getSamplingInterval());
            datasetData.put("header", dataset.getHeader());
            datasetData.put("timeRange", Map.of(
                    "start", dataset.getTimeRange().getFrom(),
                    "end", dataset.getTimeRange().getTo()
            ));
            
            mapper.writeValue(cacheFile, datasetData);
        } catch (IOException e) {
            System.err.println("Failed to save dataset to cache: " + e.getMessage());
        }
    }
    
    private static void populateDatasetFromCache(AbstractDataset dataset, Map<String, Object> data) {
        dataset.setSamplingInterval(((Number) data.get("samplingInterval")).longValue());
        
        // Convert header from List to String array
        Object headerObj = data.get("header");
        if (headerObj instanceof java.util.List) {
            java.util.List<?> headerList = (java.util.List<?>) headerObj;
            String[] headerArray = new String[headerList.size()];
            for (int i = 0; i < headerList.size(); i++) {
                headerArray[i] = headerList.get(i).toString();
            }
            dataset.setHeader(headerArray);
        }
        
        // Set time range
        Map<String, Number> timeRange = (Map<String, Number>) data.get("timeRange");
        dataset.setTimeRange(new gr.imsi.athenarc.middleware.domain.TimeRange(
                timeRange.get("start").longValue(),
                timeRange.get("end").longValue()
        ));
    }
    
    private static File getCacheFile(String source, String schema, String tableName) {
        String filename = source + "_" + (schema == null ? "" : schema + "_") + tableName + ".json";
        return Paths.get(CACHE_DIRECTORY, filename).toFile();
    }
}
