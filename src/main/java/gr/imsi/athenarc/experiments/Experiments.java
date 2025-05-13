package gr.imsi.athenarc.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.imsi.athenarc.middleware.cache.initialization.MemoryBoundedInitializationPolicy;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.middleware.datasource.config.DataSourceConfiguration;
import gr.imsi.athenarc.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.manager.CacheManager;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
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
    public String queries = null;

    @Parameter(names = "-type", description = "The type of the input (influx/postgres)")
    public String type = "influx";

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures = Arrays.asList(1); // Default measure for the dataset

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-q", description = "Query percentage factor (from the end)")
    Double q = 0.1;

    @Parameter(names = "-p", description = "Prefetching factor")
    Double prefetchingFactor = 0.0;
    
    @Parameter(names = "-a")
    private float accuracy = 0.95f;

    @Parameter(names = "-agg")
    private int aggFactor = 4;

    @Parameter(names = "-reduction")
    private int reductionFactor = 6;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder = "output";

    @Parameter(names = "-seqCount", description = "Number of queries in the sequence")
    private Integer seqCount = 50;

    @Parameter(names = "-schema", description = "PostgreSQL/InfluxDB schema name where data lay")
    private String schema = "more";

    @Parameter(names = "-table", description = "PostgreSQL/InfluxDB table name to query")
    private String table = "intel_lab_exp";

    @Parameter(names = "-viewport", converter = ViewPortConverter.class, description = "Viewport of query")
    private ViewPort viewPort = new ViewPort(1000, 600);

    @Parameter(names = "-runs", description = "Times to run each experiment workflow")
    private Integer runs = 1;

    @Parameter(names = "-queryConfig", description = "Path to query configuration properties file")
    public String queryConfigFile = "/Users/vasilisstamatopoulos/Documents/Works/ATHENA/PhD/Code/pattern-cache/config/state-transitions.properties";

    @Parameter(names = "-mode", description = "Mode: 'run' (default) or 'generate' to only create query sequence")
    private String mode = "generate";

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
        type = type.toLowerCase(Locale.ROOT);
        initOutput();
        
        if ("generate".equalsIgnoreCase(mode)) {
            generateQuerySequenceOnly();
        } else {
            timeQueries();
        }
    }

    private void timeQueries() throws IOException, SQLException {
        Preconditions.checkNotNull(type, "You must define the execution mode (tti, raw, postgres, influx).");
        for  (int run = 0; run < runs; run ++){
            Path runPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run);
            FileUtil.build(runPath.toString());            
            File outFile = Paths.get(runPath.toString(), "results.csv").toFile();
            CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
            CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            DataSource dataSource = createDatasource();

            CacheManager cacheManager = CacheManager.createDefault(dataSource);
            // Add declaration for startTime and endTime
            long startTime = dataSource.getDataset().getTimeRange().getFrom();
            long endTime = dataSource.getDataset().getTimeRange().getTo();
            if(measures == null) measures = dataSource.getDataset().getMeasures();

            Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, measures, viewPort);
            
            MemoryBoundedInitializationPolicy policy = new MemoryBoundedInitializationPolicy(
                    512 * 1024 * 1024, // 512MB memory limit
                    0.01, q);
                
                // Initialize the cache with specified measures
                policy.initialize(cacheManager.getCache(), cacheManager.getVisualQueryManager(), measures);
            
            List<TypedQuery> sequence = generateQuerySequence(q0, dataSource.getDataset());
            // Update CSV header to include query_type
            csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type", "Time (sec)");
    
            for (int i = 0; i < sequence.size(); i += 1) {
                stopwatch.start();
                TypedQuery typedQuery = sequence.get(i);
                Query query = typedQuery.getQuery();
                QueryResults queryResults;
                double time = 0;
                LOG.info("Executing query " + i + " (" + typedQuery.getUserOpType() + ") " + query.getFrom() + " - " + query.getTo());
                queryResults = cacheManager.executeQuery(query);
                time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                LOG.info("Query time: {}", time);
                csvWriter.addValue(table);
                csvWriter.addValue(i);
                csvWriter.addValue(query.getViewPort().getWidth());
                csvWriter.addValue(query.getViewPort().getHeight());
                csvWriter.addValue(DateTimeUtil.format(query.getFrom()));
                csvWriter.addValue(DateTimeUtil.format(query.getTo()));
                csvWriter.addValue(typedQuery.getUserOpType());
                csvWriter.addValue(time);
                csvWriter.writeValuesToRow();
                stopwatch.reset();
            }
            csvWriter.flush();
        }
    }

    /**
     * Method that only generates and saves a query sequence without running the actual experiment
     */
    private void generateQuerySequenceOnly() throws IOException {
        LOG.info("Running in query sequence generation mode");
        
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
        
        // Generate the query sequence
        List<TypedQuery> querySequence = generateQuerySequence(q0, dataset);
        
        LOG.info("Query sequence generation complete. {} queries generated and saved to: {}", 
                querySequence.size(), Paths.get(outFolder, "queries.txt").toString());
    }

    private List<TypedQuery> generateQuerySequence(Query q0, AbstractDataset dataset) {
        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(dataset);
        List<TypedQuery> querySequence = null;
        // If a query configuration file is provided, load and apply the settings
        if (queryConfigFile != null) {
            Properties queryProperties = readPropertiesFromFile(queryConfigFile);
            if (queryProperties != null) {
                sequenceGenerator.setProbabilitiesFromProperties(queryProperties);
            }
        }
        
        if(queries != null) {
            Preconditions.checkNotNull(queries, "No given queries.txt file");
            // querySequence = sequenceGenerator.generateQuerySequence(q0, queries);
        }
        else {
            Preconditions.checkNotNull(seqCount, "No sequence count specified.");
            querySequence = sequenceGenerator.generateQuerySequence(q0, seqCount);
            sequenceGenerator.saveQueriesToFile(querySequence, Paths.get(outFolder, "queries.txt").toString());
        }
        return querySequence;
    }

    private void initOutput() throws IOException {
        Path outFolderPath = Paths.get(outFolder);
        Path timeQueriesPath = Paths.get(outFolder, "timeQueries");
        Path typePath = Paths.get(outFolder, "timeQueries", type);
        Path tablePath = Paths.get(outFolder, "timeQueries", type, table);
        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
    }
    
    private DataSource createDatasource(){
        DataSource datasource = null;
        DataSourceConfiguration dataSourceConfiguration = null;
        Properties properties = readProperties();
        LOG.info("{}", properties);
        switch (type) {
            case "postgres":
                // dataSourceConfiguration = new PostgreSQLConfiguration.Builder()
                //     .url(properties.getProperty("postgres.url"))
                //     .username(properties.getProperty("postgres.username"))
                //     .password(properties.getProperty("postgres.password"))
                //     .schema(schema)
                //     .timeFormat(timeFormat)
                //     .table(table)
                //     .build();  
                break;
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
