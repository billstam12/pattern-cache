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
import gr.imsi.athenarc.middleware.datasource.config.SQLConfiguration;
import gr.imsi.athenarc.middleware.datasource.config.TrinoConfiguration;
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
import gr.imsi.athenarc.middleware.sketch.Sketch;
import gr.imsi.athenarc.middleware.sketch.SketchUtils;
import gr.imsi.athenarc.middleware.visual.VisualUtils;
import gr.imsi.athenarc.experiments.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Experiments {
    
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
        } else {
            runTimingExperiment();
        }
    }

    private void runTimingExperiment() throws IOException, SQLException {
        ExperimentConfig config = createExperimentConfig();
        if (config == null) {
            LOG.error("Unknown mode: {}. Supported modes are: timeCacheQueries, timeRawQueries, timeAggregateQueries, timeMinMaxCacheQueries, timeMatchRecognizeQueries, generate", mode);
            return;
        }
        initOutput(config.methodName);
        runTimingExperimentWithConfig(config);
    }

    private ExperimentConfig createExperimentConfig() {
        switch (mode.toLowerCase()) {
            case "timecachequeries":
                Preconditions.checkNotNull(method, "You must specify a method for cache queries.");
                Preconditions.checkArgument(method.equals("m4") 
                    || method.equals("m4Inf")
                    || method.equals("approxOls") 
                    || method.equals("minmax"), 
                    "Method must be either 'm4', 'approxOls' or 'minmax' for cache queries.");
                return new ExperimentConfig(method, true, method, method);
            case "timeminmaxcachequeries":
                return new ExperimentConfig("minMaxCache", true, "minmax", "firstLastInf");
            case "timerawqueries":
                return new ExperimentConfig("raw", false, "raw", null);
            case "timeaggregatequeries":
                Preconditions.checkNotNull(method, "You must specify a method for aggregate queries.");
                Preconditions.checkArgument(method.equals("firstLast") || method.equals("ols"),
                    "Method must be either 'firstLast' or 'ols' for aggregate queries.");
                return new ExperimentConfig("aggregate", false, "m4", method);
            case "timematchrecognizequeries":
                Preconditions.checkNotNull(method, "You must specify a method for match recognize queries.");
                Preconditions.checkArgument(method.equals("firstLast") || method.equals("ols"),
                    "Method must be either 'firstLast' or 'ols' for match recognize queries.");
                return new ExperimentConfig("matchRecognize", false, "m4", method);
            default:
                return null;
        }
    }

    private static class ExperimentConfig {
        final String methodName;
        final boolean useCache;
        final String visualMethod;
        final String patternMethod;

        ExperimentConfig(String methodName, boolean useCache, String visualMethod, String patternMethod) {
            this.methodName = methodName;
            this.useCache = useCache;
            this.visualMethod = visualMethod;
            this.patternMethod = patternMethod;
        }
    }

    private void runTimingExperimentWithConfig(ExperimentConfig config) throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, mode, config.methodName, type, table, "run_" + run).toString();
            FileUtil.build(resultsPath);
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long maxMemoryBytes = 100 * 1024 * 1024; // 100MB memory limit for cache
                
                CacheManager cacheManager = null;
                double initTime = 0;
                
                // Setup cache if needed
                if (config.useCache) {
                    String cacheMethod = config.visualMethod != null ? config.visualMethod : method;
                    cacheManager = CacheManager.builder(dataSource)
                        .withMaxMemory(maxMemoryBytes)
                        .withMethod(cacheMethod)
                        .withCalendarAlignment(!config.methodName.equalsIgnoreCase("minMaxCache"))
                        .build();
                    
                    // Initialize cache if specified
                    if (initCacheAllocation > 0) {
                        stopwatch.start();
                        MemoryBoundedInitializationPolicy policy = new MemoryBoundedInitializationPolicy(
                            maxMemoryBytes, 
                            initCacheAllocation,
                            cacheMethod);
                        
                        long startTime = dataSource.getDataset().getTimeRange().getFrom();
                        long endTime = dataSource.getDataset().getTimeRange().getTo();
                        if (measures == null) measures = dataSource.getDataset().getMeasures();
                        
                        Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);
                        policy.initialize(cacheManager, List.of(q0.getMeasures().get(0)));
                        initTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                        stopwatch.reset();
                    }
                }
                
                // Setup data and queries
                long startTime = dataSource.getDataset().getTimeRange().getFrom();
                long endTime = dataSource.getDataset().getTimeRange().getTo();
                if (measures == null) measures = dataSource.getDataset().getMeasures();
                
                Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);
                List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset(), false);
                
                // Write CSV headers
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type", 
                    "Init Time (sec)", "Time (sec)", "IO Count", "Cache Hits (%)", "Cache Size (bytes)");
                csvWriter.flush();
                
                // Execute queries
                for (int i = 0; i < sequence.size(); i++) {
                    executeAndRecordQuery(csvWriter, stopwatch, sequence.get(i), i, run, resultsPath, 
                        dataSource, cacheManager, config, initTime);
                }
            } catch (Exception e) {
                LOG.error("Critical error during experiment run: ", e);
                throw new RuntimeException("Error during experiment run: " + e.getMessage(), e);
            }
        }
    }

    private void executeAndRecordQuery(CsvWriter csvWriter, Stopwatch stopwatch, TypedQuery typedQuery, 
            int queryIndex, int run, String resultsPath, DataSource dataSource, CacheManager cacheManager, 
            ExperimentConfig config, double initTime) {
        
        stopwatch.start();
        Query query = typedQuery.getQuery();
        QueryResults queryResults = null;
        double time = 0;
        
        try {
            LOG.info("Executing query {} ({}) {} - {}", queryIndex, typedQuery.getUserOpType(), 
                query.getFrom(), query.getTo());
            
            queryResults = executeQueryBasedOnConfig(query, dataSource, cacheManager, config);
            
            if (query instanceof PatternQuery) {
                logMatchesToFile((PatternQuery) query, 
                    (PatternQueryResults) queryResults, config, type, dataSource, outFolder);
            }
        } catch (Exception e) {
            LOG.error("Error during query execution: ", e);
        } finally {
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            LOG.info("Query time: {}", time);
            
            if (run == 0 && queryResults != null && queryResults instanceof VisualQueryResults) {
                ((VisualQueryResults) queryResults).toMultipleCsv(
                    Paths.get(resultsPath, "query_" + queryIndex).toString());
            }
            
            recordQueryMetrics(csvWriter, query, typedQuery, queryIndex, initTime, time, 
                queryResults, cacheManager, config);
            stopwatch.reset();
        }
    }

    private QueryResults executeQueryBasedOnConfig(Query query, DataSource dataSource, 
            CacheManager cacheManager, ExperimentConfig config) throws Exception {
        
        // cache executions
        if (config.useCache) {
            if(!config.methodName.equals("minMaxCache"))
                return cacheManager.executeQuery(query);
            else {
                // use cache only for visualization
                if (query instanceof VisualQuery) {
                    return cacheManager.executeQuery(query);
                } else if (query instanceof PatternQuery) {
                    return PatternQueryExecutor.executePatternQuery((PatternQuery) query, dataSource, config.methodName, config.patternMethod);
                } else {
                    throw new IllegalArgumentException("Unknown query type: " + query.getClass().getName());
                }
            }
        }
        
        // Non-cache execution
        if (query instanceof VisualQuery) {
            return null;
            // switch (config.visualMethod) {
            //     case "raw":
            //         return VisualUtils.executeRawQuery((VisualQuery) query, dataSource);
            //     case "m4":
            //         return VisualUtils.executeM4Query((VisualQuery) query, dataSource);
            //     default:
            //         throw new IllegalArgumentException("Unknown method: " + config.visualMethod);
            // }
        } else if (query instanceof PatternQuery) {
            return PatternQueryExecutor.executePatternQuery((PatternQuery) query, dataSource, config.methodName, config.patternMethod);
        } else {
            throw new IllegalArgumentException("Unknown query type: " + query.getClass().getName());
        }
    }

    private void recordQueryMetrics(CsvWriter csvWriter, Query query, TypedQuery typedQuery, 
            int queryIndex, double initTime, double time, QueryResults queryResults, 
            CacheManager cacheManager, ExperimentConfig config) {
        
        csvWriter.addValue(table);
        csvWriter.addValue(queryIndex);
        csvWriter.addValue(query.getViewPort().getWidth());
        csvWriter.addValue(query.getViewPort().getHeight());
        csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
        csvWriter.addValue(DateTimeUtil.format(query.getTo()));
        csvWriter.addValue(typedQuery.getUserOpType());
        csvWriter.addValue(config.useCache ? initTime : -1);
        csvWriter.addValue(time);
        
        if (queryResults != null && config.useCache) {
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(queryResults.getCacheHitRatio() * 100);
            csvWriter.addValue(cacheManager.getMemoryManager().getCurrentMemoryBytes());
        } else if (queryResults != null) {
            csvWriter.addValue(""); // No IO count for non-cache
            csvWriter.addValue(""); // No cache hit ratio
            csvWriter.addValue(""); // No cache size
        } else {
            csvWriter.addValue(-1);
            csvWriter.addValue(-1);
            csvWriter.addValue(-1);
        }
        
        csvWriter.writeValuesToRow();
        csvWriter.flush();
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

    private void initOutput(String methodName) throws IOException {
        Path outFolderPath = Paths.get(outFolder);
        Path timeQueriesPath = Paths.get(outFolder, mode);
        Path methodPath = Paths.get(outFolder, mode, methodName);
        Path typePath = Paths.get(outFolder, mode, methodName, type);
        Path tablePath = Paths.get(outFolder, mode, methodName, type, table);
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
            case "postgres":
                dataSourceConfiguration = new SQLConfiguration.Builder()
                    .url(properties.getProperty("postgres.url"))
                    .username(properties.getProperty("postgres.username"))
                    .password(properties.getProperty("postgres.password"))
                    .tableName(table)
                    .timestampColumn("timestamp")
                    .idColumn("id")
                    .valueColumn("value")
                    .build();
                break;
            case "trino":
                dataSourceConfiguration = new TrinoConfiguration.Builder()
                    .url(properties.getProperty("trino.url"))
                    .username(properties.getProperty("trino.username"))
                    .password(properties.getProperty("trino.password"))
                    .tableName(table)
                    .timestampColumn("timestamp")
                    .idColumn("id")
                    .valueColumn("value")
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

    /**
     * Log pattern matches to a file for later comparison
     */
    public static void logMatchesToFile(PatternQuery query, PatternQueryResults patternQueryResults, ExperimentConfig config, String type, DataSource dataSource, String outputFolder) {

        List<List<List<Sketch>>> matches = patternQueryResults.getMatches();
        long executionTime = patternQueryResults.getExecutionTime();

        try {
            
            // Create matches directory if it doesn't exist
            Path patternMatchesDir = Paths.get(outputFolder,"pattern_matches");
            if (!Files.exists(patternMatchesDir)) {
                Files.createDirectories(patternMatchesDir);
            }
          
            Path methodDir = Paths.get(outputFolder, "pattern_matches", config.methodName);
            if (!Files.exists(methodDir)) {
                Files.createDirectories(methodDir);
            }

            Path patternMethodDir = Paths.get(outputFolder, "pattern_matches", config.methodName, config.patternMethod);
            if (!Files.exists(patternMethodDir)) {
                Files.createDirectories(patternMethodDir);
            }

            Path dbDir = Paths.get(outputFolder, "pattern_matches", config.methodName, config.patternMethod, type);
            if (!Files.exists(dbDir)) {
                Files.createDirectories(dbDir);
            }

            // Create specific directory if it doesn't exist
            Path logDir = Paths.get(outputFolder,"pattern_matches", config.methodName, config.patternMethod, type, dataSource.getDataset().getTableName());
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
            }
            
            // Create a unique filename with timestamp
            String filename = String.format("%s_%s_%d_%s.log", 
                query.getFrom(), query.getTo(), query.getMeasure(), query.getTimeUnit().toString());

            File logFile = new File(logDir.toFile(), filename);
            
            try (FileWriter writer = new FileWriter(logFile)) {
                // Write metadata
                writer.write("Query Metadata:\n");
                writer.write(String.format("Time Range: %d to %d\n", query.getFrom(), query.getTo()));
                writer.write(String.format("Measure ID: %d\n", query.getMeasure()));
                writer.write(String.format("Time Unit: %s\n", query.getTimeUnit()));
                writer.write(String.format("Execution Time: %d ms\n", executionTime));
                writer.write(String.format("Total Matches: %d\n", matches.size()));
                writer.write("\n--- Matches ---\n\n");
                
                // Write each match with simplified information
                for (int matchIdx = 0; matchIdx < matches.size(); matchIdx++) {
                    List<List<Sketch>> match = matches.get(matchIdx);
                    
                    // Calculate the overall match time range and error margin
                    long matchStart = Long.MAX_VALUE;
                    long matchEnd = Long.MIN_VALUE;
                    double totalErrorMargin = 0.0;
                    int segmentCount = 0;
                    
                    for (List<Sketch> segment : match) {
                        Sketch combinedSketch = SketchUtils.combineSketches(segment);
                        matchStart = Math.min(matchStart, combinedSketch.getFrom());
                        matchEnd = Math.max(matchEnd, combinedSketch.getTo());
                        
                        // Accumulate error margins for average calculation
                        double segmentError = combinedSketch.getAngleErrorMargin();
                        if (!Double.isNaN(segmentError) && !Double.isInfinite(segmentError)) {
                            totalErrorMargin += segmentError;
                            segmentCount++;
                        }
                    }
                    
                    // Calculate average error margin for the match
                    double averageErrorMargin = segmentCount > 0 ? totalErrorMargin / segmentCount : 0.0;

                    writer.write(String.format("Match #%d: [%d to %d] - Average Error Margin: %.2f%%\n", 
                        matchIdx + 1, matchStart, matchEnd, averageErrorMargin * 100.0));

                    for (int segmentIdx = 0; segmentIdx < match.size(); segmentIdx++) {
                        List<Sketch> segment = match.get(segmentIdx);
                        Sketch combinedSketch = SketchUtils.combineSketches(segment);
                        double segmentErrorMargin = combinedSketch.getAngleErrorMargin();
                        writer.write(String.format("  Segment %d: [%d to %d] - Error Margin: %.2f%%\n", 
                            segmentIdx, combinedSketch.getFrom(), combinedSketch.getTo(), segmentErrorMargin * 100.0));
                    }
                    writer.write("\n");
                }
            }
            
            LOG.info("Pattern matches logged to file: {}", logFile.getAbsolutePath());
            
        } catch (IOException e) {
            LOG.error("Failed to log pattern matches to file", e);
        }
    }
    
}
