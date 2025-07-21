package gr.imsi.athenarc.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.imsi.athenarc.middleware.cache.CacheManager;
import gr.imsi.athenarc.middleware.cache.initialization.MemoryBoundedInitializationPolicy;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.middleware.datasource.config.DataSourceConfiguration;
import gr.imsi.athenarc.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.pattern.PatternQueryExecutor;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.visual.VisualUtils;
import gr.imsi.athenarc.experiments.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Experiments<T> {
    
    private static final Logger LOG = LoggerFactory.getLogger(Experiments.class);

    @Parameter(names = "-queries", description = "The path of the input queries file if it exists")
    public String queries;

    @Parameter(names = "-type", description = "The type of the input (influx/postgres)")
    public String type = "influx";

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures; // Default measure for the dataset

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-q", description = "Query percentage factor (from the end)", required = false)
    Double q;

    @Parameter(names = "-p", description = "Prefetching factor", required = false)
    Double prefetchingFactor;
    
    @Parameter(names = "-a", required = false)
    private float accuracy;

    @Parameter(names = "-agg", required = false)
    private int aggFactor;

    @Parameter(names= "-initCacheAllocation", description = "Init cache percentage of memory to use")
    private double initCacheAllocation = 0.0;
    
    @Parameter(names = "-reduction", required = false)
    private int reductionFactor;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder = "output";

    @Parameter(names = "-seqCount", description = "Number of queries in the sequence", required = false)
    private Integer seqCount;

    @Parameter(names = "-schema", description = "PostgreSQL/InfluxDB schema name where data lay")
    private String schema;

    @Parameter(names = "-table", description = "PostgreSQL/InfluxDB table name to query")
    private String table;

    @Parameter(names = "-viewport", converter = ViewPortConverter.class, description = "Viewport of query", required = false)
    private ViewPort viewPort = new ViewPort(1000, 600);

    @Parameter(names = "-runs", description = "Times to run each experiment workflow", required = false)
    private Integer runs = 5;

    @Parameter(names = "-queryConfig", description = "Path to query configuration properties file", required = false)
    public String queryConfigFile;

    @Parameter(names = "-stateConfig", description = "Path to query configuration properties file", required = false)
    public String stateTransitionsFile;

    @Parameter(names = "-mode", description = "Mode: 'timeCacheQueries' (default), timeQueries (no-cache) or 'generate' to only create query sequence")
    private String mode;

    @Parameter(names = "-method", description = "Method to use for query execution (e.g., 'm4Inf', 'm4')")
    private String method;

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;
    public Experiments() {
   
    }

    public static void main(String... args) throws IOException, SQLException, NoSuchMethodException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments);
        jCommander.parse(args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, SQLException, NoSuchMethodException {
        Preconditions.checkNotNull(outFolder, "No out folder specified.");
        Preconditions.checkNotNull(type, "You must define the execution mode (postgres, influx).");

        type = type.toLowerCase(Locale.ROOT);
        if ("generate".equalsIgnoreCase(mode)) {
            generateQuerySequenceOnly();
        } else if ("timeCacheQueries".equalsIgnoreCase(mode)) {
            initOutput();
            timeCacheQueries();
        } else if("timeMinMaxCacheQueries".equalsIgnoreCase(mode)){
            method = "minmax";
            initOutput();
            timeMinMaxCacheQueries();
        } else if("timeRawQueries".equalsIgnoreCase(mode)){
            method = "raw";
            initOutput();
            timeRawQueries();
        } else {
            method = "m4";
            initOutput();
            timeM4Queries();
        }
    }

    private void timeRawQueries() throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, "timeRawQueries", method, type, table, "run_" + run).toString();
            FileUtil.build(resultsPath);            
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long startTime = dataSource.getDataset().getTimeRange().getFrom();
                long endTime = dataSource.getDataset().getTimeRange().getTo();

                if (measures == null) measures = dataSource.getDataset().getMeasures();

                Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);

                List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset(), false);
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type", "Time (sec)");
                csvWriter.flush(); // Ensure headers are written immediately
        
                for (int i = 0; i < sequence.size(); i += 1) {
                    stopwatch.start();
                    double time = 0;
                    TypedQuery typedQuery = sequence.get(i);
                    Query query = typedQuery.getQuery();
                    QueryResults queryResults = null;
                    try {
                        LOG.info("Executing query " + i + " (" + typedQuery.getUserOpType() + ") " + query.getFrom() + " - " + query.getTo());
                        if (query instanceof VisualQuery){
                            queryResults = VisualUtils.executeRawQuery((VisualQuery) query, dataSource);   
                        } else if (query instanceof PatternQuery) { 
                            continue;
                        } else {
                            throw new IllegalArgumentException("Unknown query type: " + query.getClass().getName());
                        }
                    } catch (Exception e) {
                        LOG.error("Error during query execution: ", e);
                        // Continue with the next query - we still want to record this one failed
                    } finally {
                        time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                        LOG.info("Query time: {}", time);
                        if(run == 0 && queryResults != null && queryResults instanceof VisualQueryResults) 
                            ((VisualQueryResults) queryResults).toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());

                        // Always write the row, even if the query failed
                        csvWriter.addValue(table);
                        csvWriter.addValue(i);
                        csvWriter.addValue(query.getViewPort().getWidth());
                        csvWriter.addValue(query.getViewPort().getHeight());
                        csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
                        csvWriter.addValue(DateTimeUtil.format(query.getTo()));
                        csvWriter.addValue(typedQuery.getUserOpType());
                        csvWriter.addValue(time);
                        csvWriter.writeValuesToRow();
                        csvWriter.flush(); // Force flush after each row
                        stopwatch.reset();
                    }
                }
            } catch (Exception e) {
                LOG.error("Critical error during experiment run: ", e);
                throw new RuntimeException("Error during experiment run: " + e.getMessage(), e);
            }
        }
    }



    private void timeM4Queries() throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, "timeM4Queries", method, type, table, "run_" + run).toString();
            FileUtil.build(resultsPath);            
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long startTime = dataSource.getDataset().getTimeRange().getFrom();
                long endTime = dataSource.getDataset().getTimeRange().getTo();

                if (measures == null) measures = dataSource.getDataset().getMeasures();

                Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);

                List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset(), false);
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type", "Init Time (sec)", "Time (sec)", "IO Count", "Cache Hits (%)", "Cache Size (bytes)");
                csvWriter.flush(); // Ensure headers are written immediately
        
                for (int i = 0; i < sequence.size(); i += 1) {
                    stopwatch.start();
                    double time = 0;
                    TypedQuery typedQuery = sequence.get(i);
                    Query query = typedQuery.getQuery();
                    QueryResults queryResults = null;
                    try {
                        LOG.info("Executing query " + i + " (" + typedQuery.getUserOpType() + ") " + query.getFrom() + " - " + query.getTo());
                        if (query instanceof VisualQuery){
                            queryResults = VisualUtils.executeM4Query((VisualQuery) query, dataSource);   
                        } else if (query instanceof PatternQuery) { 
                            queryResults = PatternQueryExecutor.executePatternQuery((PatternQuery) query, dataSource, "firstLast");
                            // Log matches to file
                            PatternQueryExecutor.logMatchesToFile((PatternQuery) query, (PatternQueryResults) queryResults, "ground_truth", dataSource, outFolder);
                        } else {
                            throw new IllegalArgumentException("Unknown query type: " + query.getClass().getName());
                        }
                    } catch (Exception e) {
                        LOG.error("Error during query execution: ", e);
                        // Continue with the next query - we still want to record this one failed
                    } finally {
                        time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                        LOG.info("Query time: {}", time);
                        if(run == 0 && queryResults != null && queryResults instanceof VisualQueryResults) 
                            ((VisualQueryResults) queryResults).toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());

                        // Always write the row, even if the query failed
                        csvWriter.addValue(table);
                        csvWriter.addValue(i);
                        csvWriter.addValue(query.getViewPort().getWidth());
                        csvWriter.addValue(query.getViewPort().getHeight());
                        csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
                        csvWriter.addValue(DateTimeUtil.format(query.getTo()));
                        csvWriter.addValue(typedQuery.getUserOpType());
                        csvWriter.addValue(-1);
                        csvWriter.addValue(time);
                        csvWriter.addValue(""); // Indicate no IO count
                        csvWriter.addValue(""); // Indicate no cache hit ratio
                        csvWriter.addValue(""); // Indicate no cache size
                        csvWriter.writeValuesToRow();
                        csvWriter.flush(); // Force flush after each row
                        stopwatch.reset();
                    }
                }
            } catch (Exception e) {
                LOG.error("Critical error during experiment run: ", e);
                throw new RuntimeException("Error during experiment run: " + e.getMessage(), e);
            }
        }
    }
    
    private void timeCacheQueries() throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, "timeCacheQueries", method, type, table, "run_" + run).toString();
        
            FileUtil.build(resultsPath);            
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long maxMemoryBytes = 100 * 1024 * 1024; // 100MB memory limit for cache
                CacheManager cacheManager = CacheManager.builder(dataSource)
                    .withMaxMemory(maxMemoryBytes)
                    .withMethod(method)
                    .build();

                // Add declaration for startTime and endTime
                long startTime = dataSource.getDataset().getTimeRange().getFrom();
                long endTime = dataSource.getDataset().getTimeRange().getTo();
                if(measures == null) measures = dataSource.getDataset().getMeasures();

                Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);
                
                stopwatch.start();
                if(initCacheAllocation > 0) {
                    MemoryBoundedInitializationPolicy policy = new MemoryBoundedInitializationPolicy(
                        maxMemoryBytes, 
                        initCacheAllocation,
                        method);
                    policy.initialize(cacheManager, measures);
                }
                double initTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                stopwatch.reset();

                List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset(), false);
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type","Init Time (sec)", "Time (sec)", "IO Count", "Cache Hits (%)", "Cache Size (bytes)");
                csvWriter.flush(); // Ensure headers are written immediately
        
                for (int i = 0; i < sequence.size(); i += 1) {
                    stopwatch.start();
                    TypedQuery typedQuery = sequence.get(i);
                    Query query = typedQuery.getQuery();
                    QueryResults queryResults = null;
                    double time = 0;
                    
                    try {
                        LOG.info("Executing query " + i + " (" + typedQuery.getUserOpType() + ") " + query.getFrom() + " - " + query.getTo());
                        queryResults = cacheManager.executeQuery(query);
                        if (query instanceof PatternQuery) { 
                            // Log matches to file
                            PatternQueryExecutor.logMatchesToFile((PatternQuery)query, (PatternQueryResults) queryResults, method, dataSource, outFolder);
                        }
                    } catch (Exception e) {
                        LOG.error("Error during query execution: ", e);
                        // Continue with the next query
                    } finally {
                        time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                        LOG.info("Query time: {}", time);
                        if(run == 0 && queryResults != null && queryResults instanceof VisualQueryResults) 
                            ((VisualQueryResults) queryResults).toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());

                        // Always write the row, even if the query failed
                        csvWriter.addValue(table);
                        csvWriter.addValue(i);
                        csvWriter.addValue(query.getViewPort().getWidth());
                        csvWriter.addValue(query.getViewPort().getHeight());
                        csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
                        csvWriter.addValue(DateTimeUtil.format(query.getTo()));
                        csvWriter.addValue(typedQuery.getUserOpType());
                        csvWriter.addValue(initTime);
                        csvWriter.addValue(time);                        
                        // Add IO count and cache hit ratio if available, otherwise add -1
                        if (queryResults != null) {
                            csvWriter.addValue(queryResults.getIoCount());
                            csvWriter.addValue(queryResults.getCacheHitRatio() * 100); // Convert to percentage
                            csvWriter.addValue(cacheManager.getMemoryManager().getCurrentMemoryBytes()); // Cache size in bytes
                        } else {
                            csvWriter.addValue(-1); // Indicate error for IO count
                            csvWriter.addValue(-1); // Indicate error for cache hit ratio
                            csvWriter.addValue(-1); // Indicate error for cache size
                        }
                        
                        csvWriter.writeValuesToRow();
                        csvWriter.flush(); // Force flush after each row
                        
                        stopwatch.reset();
                    }
                }
            } catch (Exception e) {
                LOG.error("Critical error during experiment run: ", e);
                throw new RuntimeException("Error during experiment run: " + e.getMessage(), e);
            }
        }
    }

    private void timeMinMaxCacheQueries() throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, "timeMinMaxCacheQueries", method, type, table, "run_" + run).toString();
            FileUtil.build(resultsPath);            
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long maxMemoryBytes = 100 * 1024 * 1024; // 100MB memory limit for cache
                
                CacheManager cacheManager = CacheManager.builder(dataSource)
                    .withMaxMemory(maxMemoryBytes)
                    .withMethod("minmax")
                    .withCalendarAlignment(false)
                    .build();

                // Add declaration for startTime and endTime
                long startTime = dataSource.getDataset().getTimeRange().getFrom();
                long endTime = dataSource.getDataset().getTimeRange().getTo();
                if(measures == null) measures = dataSource.getDataset().getMeasures();

                Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);
                
                stopwatch.start();
                if(initCacheAllocation > 0) {
                    MemoryBoundedInitializationPolicy policy = new MemoryBoundedInitializationPolicy(
                        maxMemoryBytes, 
                        initCacheAllocation,
                        method);
                    
                    // Initialize the cache with specified measures
                    policy.initialize(cacheManager, List.of(q0.getMeasures().get(0)));   
                }
                double initTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                stopwatch.reset();
                
                List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset(), false);
                // Update CSV header to include query_type
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type", "Init Time (sec)", "Time (sec)", "IO Count", "Cache Hits (%)", "Cache Size (bytes)");
                csvWriter.flush(); // Ensure headers are written immediately
        
                for (int i = 0; i < sequence.size(); i += 1) {
                    stopwatch.start();
                    TypedQuery typedQuery = sequence.get(i);
                    Query query = typedQuery.getQuery();
                    double time = 0;
                    QueryResults queryResults = null;
                    try {
                        LOG.info("Executing query " + i + " (" + typedQuery.getUserOpType() + ") " + query.getFrom() + " - " + query.getTo());
                        if (query instanceof VisualQuery){
                            queryResults = cacheManager.executeQuery(query);
                        } else if (query instanceof PatternQuery) { 
                            queryResults = 
                                PatternQueryExecutor.executePatternQuery((PatternQuery) query, dataSource, "firstLastInf");
                                // Log matches to file
                            PatternQueryExecutor.logMatchesToFile((PatternQuery)query, (PatternQueryResults) queryResults, "minmaxcache", dataSource, outFolder);
                        } else {
                            throw new IllegalArgumentException("Unknown query type: " + query.getClass().getName());
                        }
                    } catch (Exception e) {
                        LOG.error("Error during query execution: ", e);
                        // Continue with the next query
                    } finally {
                        time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

                        LOG.info("Query time: {}", time);
                        if(run == 0 && queryResults != null && queryResults instanceof VisualQueryResults) 
                            ((VisualQueryResults) queryResults).toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());

                        // Always write the row, even if the query failed
                        csvWriter.addValue(table);
                        csvWriter.addValue(i);
                        csvWriter.addValue(query.getViewPort().getWidth());
                        csvWriter.addValue(query.getViewPort().getHeight());
                        csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
                        csvWriter.addValue(DateTimeUtil.format(query.getTo()));
                        csvWriter.addValue(typedQuery.getUserOpType());
                        csvWriter.addValue(initTime);
                        csvWriter.addValue(time);
                       // Add IO count and cache hit ratio if available, otherwise add -1
                        if (queryResults != null) {
                            csvWriter.addValue(queryResults.getIoCount());
                            csvWriter.addValue(queryResults.getCacheHitRatio() * 100); // Convert to percentage
                            csvWriter.addValue(cacheManager.getMemoryManager().getCurrentMemoryBytes()); // Cache size in bytes
                        } else {
                            csvWriter.addValue(-1); // Indicate error for IO count
                            csvWriter.addValue(-1); // Indicate error for cache hit ratio
                            csvWriter.addValue(-1); // Indicate error for cache size
                        }
                        csvWriter.writeValuesToRow();
                        csvWriter.flush(); // Force flush after each row
                        
                        stopwatch.reset();
                    }
                }
            } catch (Exception e) {
                LOG.error("Critical error during experiment run: ", e);
                throw new RuntimeException("Error during experiment run: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Method that only generates and saves a query sequence without running the actual experiment
     */
    private void generateQuerySequenceOnly() throws IOException {
        LOG.info("Running in query sequence generation mode");
        Preconditions.checkArgument(queries == null,
            "Cannot specify queries file when generating query sequence");

        // Create data source to access the dataset
        DataSource dataSource = createDatasource();
        AbstractDataset dataset = dataSource.getDataset();
        
        // Get measures from dataset if not provided
        if(measures == null) measures = dataset.getMeasures();

        // Initialize query parameters
        long startTime = dataset.getTimeRange().getFrom();
        long endTime = dataset.getTimeRange().getTo();
        
        // Create initial query
        Query q0 = initiliazeQ0(dataset, startTime, endTime, accuracy, measures, viewPort);
        generateQuerySequence(q0, dataset, true);

    }

    private List<TypedQuery> generateQuerySequence(Query q0, AbstractDataset dataset, boolean save) {
        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(dataset);
        if(queries != null) {
            return sequenceGenerator.generateQuerySequenceFromFile(queries);
        }

        // Check that not both configuration files are provided
        Preconditions.checkArgument(queryConfigFile == null || stateTransitionsFile == null, 
            "Cannot specify both queryConfig and stateTransitions files simultaneously");

        // If a query configuration file is provided, load and apply the settings
        if (queryConfigFile != null) {
            Properties queryProperties = readPropertiesFromFile(queryConfigFile);
            if (queryProperties != null) {
                sequenceGenerator.setProbabilitiesFromProperties(queryProperties);
            }
        } else if (stateTransitionsFile != null) {
            Properties stateTransitionsProperties = readPropertiesFromFile(stateTransitionsFile);
            if (stateTransitionsProperties != null) {
                sequenceGenerator.setStateTransitionsFromProperties(stateTransitionsProperties);
            }
        }
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        List<TypedQuery> querySequence = sequenceGenerator.generateQuerySequence(q0, seqCount);
        String per = q == 1 ? "guided" : ((int) (q * 100)) + "p";
        String genFile = dataset.getTableName() + "_queries_" + per + ".txt";
        if(save) {
            sequenceGenerator.saveQueriesToFile(querySequence, Paths.get(outFolder, genFile).toString());
            LOG.info("Query sequence generation complete. {} queries generated and saved to: {}", 
                querySequence.size(), Paths.get(outFolder, genFile).toString());
        }
        return querySequence;
        
    }

    private void initOutput() throws IOException {
        Path outFolderPath = Paths.get(outFolder);
        Path timeQueriesPath = Paths.get(outFolder, mode);
        Path methodPath = Paths.get(outFolder, mode, method);
        Path typePath = Paths.get(outFolder, mode, method, type);
        Path tablePath = Paths.get(outFolder, mode, method, type, table);
        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(methodPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
    }
    
    private DataSource createDatasource(){
        DataSource datasource = null;
        DataSourceConfiguration dataSourceConfiguration = null;
        Properties properties = readProperties();
        LOG.info("{}", properties);
        switch (type) {
            case "influx":
                dataSourceConfiguration = new InfluxDBConfiguration.Builder()
                    .url(properties.getProperty("influxdb.url"))
                    .org(properties.getProperty("influxdb.org"))
                    .token(properties.getProperty("influxdb.token"))
                    .bucket(schema)
                    .timeFormat(timeFormat)
                    .measurement(table)
                    .build();   
                break;
            default:
                break;
            
        }
        datasource = DataSourceFactory.createDataSource(dataSourceConfiguration);
        return datasource;
    }

    private Query initiliazeQ0(AbstractDataset dataset, long startTime, long endTime, float accuracy, List<Integer> measures, ViewPort viewPort){
        // If query percent given. Change start and end times based on it
        if(q != null){
            startTime = dataset.getTimeRange().getTo() - (long) (q * (dataset.getTimeRange().getTo() - dataset.getTimeRange().getFrom()));
            endTime = (dataset.getTimeRange().getTo());
        }
        return new VisualQuery(startTime, endTime, measures, viewPort.getWidth(), viewPort.getHeight(), accuracy);
    }


    public static Properties readProperties(){
        Properties properties = new Properties();
        // Load from the resources folder
        // "/application.properties" assumes the file is at src/main/resources/application.properties
        try (InputStream input = Experiments.class.getResourceAsStream("/application.properties")) {
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

    /**
     * Read properties from a specific file
     * @param filePath Path to the properties file
     * @return Properties object or null if file cannot be read
     */
    public static Properties readPropertiesFromFile(String filePath) {
        Properties properties = new Properties();
        try (FileReader reader = new FileReader(filePath)) {
            properties.load(reader);
            LOG.info("Successfully loaded query configuration from: {}", filePath);
            return properties;
        } catch (Exception ex) {
            LOG.error("Unable to read query configuration file: {}", filePath, ex);
            return null;
        }
    }
    
}
