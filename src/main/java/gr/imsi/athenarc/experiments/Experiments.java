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
import gr.imsi.athenarc.middleware.datasource.config.DuckDBConfiguration;
import gr.imsi.athenarc.middleware.datasource.config.SQLConfiguration;
import gr.imsi.athenarc.middleware.datasource.config.TrinoConfiguration;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.ViewPort;
import gr.imsi.athenarc.middleware.pattern.BoundStatsLogger;
import gr.imsi.athenarc.middleware.pattern.MatchingStrategy;
import gr.imsi.athenarc.middleware.pattern.PatternEvaluator;
import gr.imsi.athenarc.middleware.pattern.PatternMatch;
import gr.imsi.athenarc.middleware.pattern.PatternUtils;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.QueryResults;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.refinement.RefinementScope;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;
import gr.imsi.athenarc.middleware.visual.VisualUtils;
import gr.imsi.athenarc.experiments.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Experiments {
    
    private static final Logger LOG = LoggerFactory.getLogger(Experiments.class);

    @Parameter(names = "-queries", description = "The path of the input queries file if it exists")
    public String queries;

    @Parameter(names = "-type", description = "The type of the input (postgres/timescale/trino/duckdb)")
    public String type = "duckdb";

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures; // Default measure for the dataset

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-p", description = "Prefetching factor", required = false)
    Double prefetchingFactor;

    @Parameter(names = "-a", required = false)
    private float accuracy;

    @Parameter(names = "-agg", required = false, description = "Initial aggregation factor (sub-buckets per main bucket). Default 4.")
    private int aggFactor = 4;

    @Parameter(names= "-initCacheAllocation", description = "Init cache percentage of memory to use")
    private double initCacheAllocation = 0.0;

    @Parameter(names = "-reduction", required = false)
    private int reductionFactor;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder = "output";

    @Parameter(names = "-schema", description = "Schema name where data lives")
    private String schema;

    @Parameter(names = "-table", description = "Table name to query")
    private String table;

    @Parameter(names = "-csv", description = "DuckDB only: path to a CSV that is imported into the table on connect")
    private String csv;

    @Parameter(names = "-cacheDb", description = "DuckDB only: materialize the CSV into a persistent on-disk table. When false (default) an in-memory view re-reads the CSV on every query.")
    private boolean cacheDb = true;

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

    @Parameter(names = "-method", description = "Method to use for query execution (e.g., 'm4', 'approxOls')")
    private String method;

    @Parameter(names = "-adaptation", description = "Enable adaptation for cache methods that support it (default true)")
    private boolean adaptation = false;

    @Parameter(names = "-relaxedCacheReuse", description = "When true, pattern cache lookup admits cached sub-buckets that straddle the new query's outer-bucket grid (D2 shared partial). Default false.")
    private boolean relaxedCacheReuse = false;

    @Parameter(names = "-refinementScope", description = "Picks the executor for BOTH visual and pattern queries: 'scoped' (default; refines only the regions that need it + scoped full-accuracy fallback) or 'full' (single batched fetch at chosen α + full-range full-accuracy fallback).")
    private String refinementScope = "scoped";

    @Parameter(names = "-maxRefinementSteps", description = "Scoped executors only: number of doubling refinements before falling through to the full-accuracy resolver (strict-OLS for pattern, M4 for visual). Default 20 ≈ refine until cap. Lower to force the fallback earlier.")
    private int maxRefinementSteps = 20;

    @Parameter(names = "-calendarAlignment", description = "When true (default), sub-bucket widths are snapped to the next calendar level (s/min/h/d); when false, widths are kept in raw milliseconds (MinMaxCache-style bucketing). Used to compare VASTA's calendar-aligned bucketing against the MinMaxCache baseline.", arity = 1)
    private boolean calendarAlignment = true;

    @Parameter(names = "-logBoundStats", description = "D1/D2 bound-tightness experiment: when true, dump per-sketch slope-interval stats to bound_stats/run_N.csv at the initial aggregation factor of every pattern query. Default false.")
    private boolean logBoundStats = false;

    @Parameter(names = "-matchSelection", description = "Pattern matcher selection: 'longest' (default; LONGEST + AFTER_MATCH_END non-overlap) or 'all' (BFS enumerates every valid match; bypasses selection/advancement). 'all' is used by the §5 recall-attribution experiment.")
    private String matchSelection = "longest";

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
        Preconditions.checkNotNull(type, "You must define the data-source type (postgres, timescale, trino, duckdb).");
        Preconditions.checkNotNull(queries,
                "No queries file specified (-queries). Generate one with scripts/queries/generate_all.sh.");

        type = type.toLowerCase(Locale.ROOT);

        if ("all".equalsIgnoreCase(matchSelection)) {
            PatternEvaluator.setMatchingStrategy(MatchingStrategy.ALL);
        } else if ("longest".equalsIgnoreCase(matchSelection)) {
            PatternEvaluator.setMatchingStrategy(MatchingStrategy.SELECTION);
        } else {
            throw new IllegalArgumentException("-matchSelection must be 'longest' or 'all' (got '" + matchSelection + "')");
        }

        runTimingExperiment();
    }

    private void runTimingExperiment() throws IOException, SQLException {
        ExperimentConfig config = createExperimentConfig();
        if (config == null) {
            LOG.error("Unknown mode: {}. Supported modes are: timeCacheQueries, timeRawQueries, timeAggregateQueries, timeMatchRecognizeQueries", mode);
            return;
        }
        // The calling script is responsible for creating outFolder. Java only
        // creates per-run subdirs and pattern-match dirs inside it.
        if (!Files.isDirectory(Paths.get(outFolder))) {
            throw new IOException("output folder does not exist (calling script should mkdir -p it): " + outFolder);
        }
        runTimingExperimentWithConfig(config);
    }

    private ExperimentConfig createExperimentConfig() {
        switch (mode.toLowerCase()) {
            case "timecachequeries":
                Preconditions.checkNotNull(method, "You must specify a method for cache queries.");
                // Accept "minmax" at the CLI boundary and normalize to the
                // camelCase "minMax" the cache framework matches on
                // (CacheUtils / TimeSeriesSpanFactory / AggregationFunctionsConfig).
                if (method.equalsIgnoreCase("minmax")) {
                    method = "minMax";
                }
                Preconditions.checkArgument(method.equals("m4")
                    || method.equals("minMax")
                    || method.equals("approxOls")
                    || method.equals("ols"),
                    "Method must be one of 'm4', 'minMax', 'approxOls' or 'ols' for cache queries.");
                Preconditions.checkArgument(!adaptation || method.equals("approxOls"),
                    "Adaptation can only be true for the 'approxOls' method");
                Preconditions.checkArgument(aggFactor >= 1,
                    "-agg must be >= 1 (got %s)", aggFactor);
                return new ExperimentConfig("cache", true, method, method);
            case "timerawqueries":
                return new ExperimentConfig("raw", false, "raw", null);
            case "timeaggregatequeries":
                Preconditions.checkNotNull(method, "You must specify a method for aggregate queries.");
                Preconditions.checkArgument(method.equals("ols"),
                    "Method must be 'ols' for aggregate queries (no-cache OLS baseline).");
                return new ExperimentConfig("aggregate", false, "m4", method);
            case "timematchrecognizequeries":
                Preconditions.checkNotNull(method, "You must specify a method for match recognize queries.");
                Preconditions.checkArgument(method.equals("ols"),
                    "Method must be 'ols' for match recognize queries.");
                return new ExperimentConfig("matchRecognize", false, "m4", method);
            default:
                return null;
        }
    }

    private static class ExperimentConfig {
        final String type;
        final boolean useCache;
        final String visualMethod;
        final String patternMethod;

        ExperimentConfig(String type, boolean useCache, String visualMethod, String patternMethod) {
            this.type = type;
            this.useCache = useCache;
            this.visualMethod = visualMethod;
            this.patternMethod = patternMethod;
        }
    }

    private void runTimingExperimentWithConfig(ExperimentConfig config) throws IOException, SQLException {
        for (int run = 0; run < runs; run++) {
            String resultsPath = Paths.get(outFolder, "run_" + run).toString();
            FileUtil.build(resultsPath);
            File outFile = Paths.get(resultsPath, "results.csv").toFile();
            if (logBoundStats) {
                BoundStatsLogger.configure(outFolder, run);
            }
            
            try (FileWriter fileWriter = new FileWriter(outFile, false)) {
                CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
                CsvWriter csvWriter = new CsvWriter(fileWriter, csvWriterSettings);
                
                Stopwatch stopwatch = Stopwatch.createUnstarted();
                DataSource dataSource = createDatasource();
                long maxMemoryBytes = 1000 * 1024 * 1024; // 100MB memory limit for cache
                
                CacheManager cacheManager = null;
                double initTime = 0;
                
                // Setup cache if needed
                if (config.useCache) {
                    String cacheMethod = config.visualMethod != null ? config.visualMethod : method;
                    cacheManager = CacheManager.builder(dataSource)
                        .withMaxMemory(maxMemoryBytes)
                        .withMethod(cacheMethod)
                        .withCalendarAlignment(calendarAlignment)
                        .withRelaxedCacheReuse(relaxedCacheReuse)
                        .withRefinementScope(RefinementScope.from(refinementScope))
                        .withMaxRefinementSteps(maxRefinementSteps)
                        .withAdaptation(adaptation)
                        .withInitialAggregationFactor(aggFactor)
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
                
                // Setup measures + load the query sequence from file.
                if (measures == null) measures = dataSource.getDataset().getMeasures();
                List<TypedQuery> sequence = QueryFileLoader.load(queries);
                
                // Write CSV headers
                csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "query_type",
                    "Init Time (sec)", "Time (sec)", "IO Count", "Cache Hits (%)", "Cache Size (bytes)",
                    "accuracy", "initialAggFactor", "finalAggFactor", "refinementTriggered", "fallbackTriggered",
                    "errorBefore", "errorAfter", "ambiguousAfter");
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
            // Tag any bound-stats rows the executor emits with this query's index.
            BoundStatsLogger.setCurrentQuery(queryIndex);
            queryResults = executeQueryBasedOnConfig(query, dataSource, cacheManager, config);
            
            if (query instanceof PatternQuery) {
                logMatchesToFile((PatternQuery) query,
                    (PatternQueryResults) queryResults, outFolder, queryIndex);
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
            return cacheManager.executeQuery(query);
        }

        // Non-cache execution
        if (query instanceof VisualQuery) {
            // return null;
            switch (config.visualMethod) {
                case "raw":
                    return VisualUtils.executeRawQuery((VisualQuery) query, dataSource);
                case "m4":
                    return VisualUtils.executeM4Query((VisualQuery) query, dataSource);
                default:
                    throw new IllegalArgumentException("Unknown method: " + config.visualMethod);
            }
        } else if (query instanceof PatternQuery) {
            return PatternUtils.executePatternQuery((PatternQuery) query, dataSource, config.type, config.patternMethod);
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
        csvWriter.addValue(config.useCache ? (initTime > 0 ? initTime : "")  : "");
        csvWriter.addValue(time);
        
        if (queryResults != null && config.useCache) {
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(queryResults.getCacheHitRatio() * 100);
            csvWriter.addValue(cacheManager.getMemoryManager().getCurrentMemoryBytes());
        } else if (queryResults != null) {
            // Non-cache path: the executors (executeRawQuery / executeM4Query /
            // executePatternQuery / MatchRecognizeQueryExecutor) now set
            // ioCount as a datapoint-equivalent count, so write it through.
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(""); // No cache hit ratio
            csvWriter.addValue(""); // No cache size
        } else {
            csvWriter.addValue(-1);
            csvWriter.addValue(-1);
            csvWriter.addValue(-1);
        }

        // Pattern-only columns: accuracy (from query) + refinement stats (from results).
        // Visual/other query types get blanks; the notebook treats missing values as N/A.
        if (query instanceof PatternQuery && queryResults instanceof PatternQueryResults) {
            PatternQuery pq = (PatternQuery) query;
            PatternQueryResults pr = (PatternQueryResults) queryResults;
            csvWriter.addValue(pq.getAccuracy());
            csvWriter.addValue(pr.getInitialAggFactor());
            csvWriter.addValue(pr.getFinalAggFactor());
            csvWriter.addValue(pr.isRefinementTriggered());
            csvWriter.addValue(pr.isFallbackTriggered());
            csvWriter.addValue(pr.getErrorBefore());
            csvWriter.addValue(pr.getErrorAfter());
            csvWriter.addValue(pr.getAmbiguousAfter());
        } else {
            for (int i = 0; i < 8; i++) csvWriter.addValue("");
        }

        csvWriter.writeValuesToRow();
        csvWriter.flush();
    }


private DataSource createDatasource(){
        DataSource datasource = null;
        DataSourceConfiguration dataSourceConfiguration = null;
        Properties properties = readProperties();
        LOG.info("{}", properties);
        switch (type) {
            case "postgres":
                dataSourceConfiguration = new SQLConfiguration.Builder()
                    .url(resolveSqlUrl(properties, "postgres"))
                    .username(properties.getProperty("postgres.username"))
                    .password(properties.getProperty("postgres.password"))
                    .schemaName(schema)
                    .tableName(table)
                    .timestampColumn("timestamp")
                    .idColumn("id")
                    .valueColumn("value")
                    .csvPath(csv)
                    .build();
                break;
            case "timescale":
                // Same JDBC/dialect path as Postgres (Timescale is a Postgres extension);
                // the hypertable flag tells the bootstrap to create a hypertable + compress.
                dataSourceConfiguration = new SQLConfiguration.Builder()
                    .url(resolveSqlUrl(properties, "timescale"))
                    .username(properties.getProperty("timescale.username"))
                    .password(properties.getProperty("timescale.password"))
                    .schemaName(schema)
                    .tableName(table)
                    .timestampColumn("timestamp")
                    .idColumn("id")
                    .valueColumn("value")
                    .csvPath(csv)
                    .hypertable(true)
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
            case "duckdb":
                dataSourceConfiguration = DuckDBConfiguration.builder()
                    .url(properties.getProperty("duckdb.url", "jdbc:duckdb:"))
                    .csvPath(csv)
                    .schemaName(schema)
                    .tableName(table)
                    .timestampColumn("timestamp")
                    .idColumn("id")
                    .valueColumn("value")
                    .timeFormat(timeFormat)
                    .cacheDb(cacheDb)
                    .build();
                break;
            default:
                break;

        }
        datasource = DataSourceFactory.createDataSource(dataSourceConfiguration);
        return datasource;
    }

    private Query initiliazeQ0(AbstractDataset dataset, long startTime, long endTime, float accuracy, List<Integer> measures, ViewPort viewPort){
        return new VisualQuery(startTime, endTime, measures, viewPort.getWidth(), viewPort.getHeight(), accuracy);
    }


    /**
     * Resolution order: {@code -Dconfig=<path>} → {@code ./application.properties}
     * (cwd) → the {@code application.properties} bundled inside the JAR. The first
     * file that exists wins; the bundled copy ships sensible defaults so a fresh
     * checkout still runs without manual setup.
     */
    public static Properties readProperties(){
        Properties properties = new Properties();
        String override = System.getProperty("config");
        java.io.File external = (override != null && !override.isEmpty())
                ? new java.io.File(override)
                : new java.io.File("application.properties");
        if (external.isFile()) {
            try (java.io.InputStream in = new java.io.FileInputStream(external)) {
                properties.load(in);
                LOG.info("Loaded config from {}", external.getAbsolutePath());
                return properties;
            } catch (Exception ex) {
                LOG.error("Failed to read {} — falling back to bundled defaults", external, ex);
            }
        }
        try (InputStream input = Experiments.class.getResourceAsStream("/application.properties")) {
            if (input == null) {
                LOG.error("Sorry, unable to find application.properties in resources.");
                return null;
            }
            properties.load(input);
            LOG.info("Loaded config from bundled /application.properties");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return properties;
    }

    /**
     * Resolve the JDBC URL for {@code prefix} (e.g. "postgres", "timescale").
     * Probes {@code <prefix>.url} with a short login timeout; on connection
     * failure returns {@code <prefix>.url.fallback}. The probe uses the same
     * credentials the real builder will use, so an auth mismatch surfaces here.
     */
    private static String resolveSqlUrl(Properties properties, String prefix) {
        String primary = properties.getProperty(prefix + ".url");
        String fallback = properties.getProperty(prefix + ".url.fallback");
        String user = properties.getProperty(prefix + ".username");
        String password = properties.getProperty(prefix + ".password");
        if (primary == null) return fallback;
        if (fallback == null) return primary;
        int prevTimeout = DriverManager.getLoginTimeout();
        try {
            DriverManager.setLoginTimeout(3);
            try (Connection probe = DriverManager.getConnection(primary, user, password)) {
                LOG.info("Using primary {} url: {}", prefix, primary);
                return primary;
            } catch (SQLException e) {
                LOG.warn("Primary {} url unreachable ({}); falling back to {}",
                        prefix, e.getMessage(), fallback);
                return fallback;
            }
        } finally {
            DriverManager.setLoginTimeout(prevTimeout);
        }
    }

    /**
     * Log pattern matches to a file for later comparison
     */
    /** Short pattern fingerprint mirroring the {@code len:minDeg:maxDeg[;...]} input syntax,
     *  with {@code n} for negative degrees and {@code -} as separators (filename-safe). */
    private static String patternFingerprint(PatternQuery query) {
        List<PatternNode> nodes = query.getPatternNodes();
        if (nodes == null || nodes.isEmpty()) return "any";
        StringBuilder sb = new StringBuilder();
        for (PatternNode node : nodes) {
            if (sb.length() > 0) sb.append(';');
            if (node instanceof SingleNode) {
                SegmentSpecification spec = ((SingleNode) node).getSpec();
                TimeFilter tf = spec.getTimeFilter();
                ValueFilter vf = spec.getValueFilter();
                String len = tf.getTimeLow() == tf.getTimeHigh()
                        ? Integer.toString(tf.getTimeLow())
                        : tf.getTimeLow() + "to" + tf.getTimeHigh();
                String filt = vf.isValueAny()
                        ? "any"
                        : sanitizeDeg(vf.getMinDegree()) + "-" + sanitizeDeg(vf.getMaxDegree());
                sb.append(len).append('-').append(filt);
            } else {
                sb.append("grp");
            }
        }
        return sb.toString();
    }

    private static String sanitizeDeg(double d) {
        long rounded = Math.round(d);
        return rounded < 0 ? "n" + Math.abs(rounded) : Long.toString(rounded);
    }

    public static void logMatchesToFile(PatternQuery query, PatternQueryResults patternQueryResults, String outputFolder, int queryIndex) {

        List<PatternMatch> matches = patternQueryResults.getMatches();
        List<PatternMatch> candidateMatches = patternQueryResults.getCandidateMatches();
        long executionTime = patternQueryResults.getExecutionTime();

        try {
            // Pattern matches sit flat under the per-invocation outFolder; the
            // dataset/db/mode/method discriminators are all in the outFolder
            // path itself (composed by the calling script).
            Path logDir = Paths.get(outputFolder, "pattern_matches");
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
            }

            String filename = String.format("q%04d_%s_%s_%d_%s_%s.log",
                queryIndex, query.getFrom(), query.getTo(),
                query.getMeasure(), query.getTimeUnit().toString(), patternFingerprint(query));

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

                for (PatternMatch match : matches) {
                    match.writeToFile(writer);
                }

                // On fallback queries the returned matches are OLS's. Log the
                // approxOls snapshot alongside so analysis can score approxOls'
                // standalone quality on the queries where it was overridden.
                if (patternQueryResults.isFallbackTriggered()) {
                    writer.write(String.format("\n--- Candidate (approxOls) — %d matches ---\n\n",
                            candidateMatches.size()));
                    for (PatternMatch match : candidateMatches) {
                        match.writeToFile(writer, "Candidate");
                    }
                }
            }
            
            LOG.info("Pattern matches logged to file: {}", logFile.getAbsolutePath());
            
        } catch (IOException e) {
            LOG.error("Failed to log pattern matches to file", e);
        }
    }
    
}
