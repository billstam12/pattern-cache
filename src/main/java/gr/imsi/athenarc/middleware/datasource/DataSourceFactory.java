package gr.imsi.athenarc.middleware.datasource;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.datasource.config.*;
import gr.imsi.athenarc.middleware.datasource.connection.*;
import gr.imsi.athenarc.middleware.datasource.dataset.*;
import gr.imsi.athenarc.middleware.datasource.duckdb.DuckDBDatasource;
import gr.imsi.athenarc.middleware.datasource.executor.*;
import gr.imsi.athenarc.middleware.datasource.sql.SQLDatasource;
import gr.imsi.athenarc.middleware.datasource.trino.TrinoDatasource;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.domain.TimeRange;

public class DataSourceFactory {

    public static final Logger LOG = LoggerFactory.getLogger(DataSourceFactory.class);
    public static DataSource createDataSource(DataSourceConfiguration config) {
        if (config instanceof SQLConfiguration) {
            return createSQLDataSource((SQLConfiguration) config);
        } else if (config instanceof TrinoConfiguration) {
            return createTrinoDataSource((TrinoConfiguration) config);
        } else if (config instanceof DuckDBConfiguration) {
            return createDuckDBDataSource((DuckDBConfiguration) config);
        }
        throw new IllegalArgumentException("Unsupported data source configuration");
    }

    private static DataSource createDuckDBDataSource(DuckDBConfiguration config) {
        // When caching is off, ignore any persistent url and stay in-memory so
        // nothing is written to disk between runs.
        String url = config.isCacheDb() ? config.getUrl() : "jdbc:duckdb:";
        JDBCConnection jdbcConnection = new JDBCConnection(url, null, null);
        jdbcConnection.connect();
        SQLQueryExecutor executor = new SQLQueryExecutor(jdbcConnection);

        if (config.getCsvPath() != null) {
            String table = config.getTableName();
            String csv = config.getCsvPath().replace("'", "''");
            if (config.isCacheDb()) {
                LOG.info("Importing CSV {} into persistent DuckDB table {}", csv, table);
                executor.executeStatement(
                    "CREATE TABLE IF NOT EXISTS " + table
                    + " AS SELECT * FROM read_csv_auto('" + csv + "')");
            } else {
                LOG.info("Creating DuckDB view over CSV {} (re-read per query, not cached)", csv);
                executor.executeStatement(
                    "CREATE OR REPLACE VIEW " + table
                    + " AS SELECT * FROM read_csv_auto('" + csv + "')");
            }
        }

        SQLDataset dataset;
        String cacheKey = config.getSchemaName() == null ? "_" : config.getSchemaName();
        if (DatasetCache.hasDataset("duckdb", cacheKey, config.getTableName())) {
            dataset = (SQLDataset) DatasetCache.getDataset("duckdb", cacheKey, config.getTableName());
            dataset.setTimeFormat(config.getTimeFormat());
            dataset.setIdColumn(config.getIdColumn());
            dataset.setValueColumn(config.getValueColumn());
            dataset.setTimestampColumn(config.getTimestampColumn());
        } else {
            dataset = new SQLDataset(
                cacheKey,
                config.getTableName(),
                config.getTimestampColumn(),
                config.getIdColumn(),
                config.getValueColumn(),
                config.getTimeFormat()
            );
            fillSQLDatasetInfo(dataset, executor);
            DatasetCache.saveDataset("duckdb", dataset);
        }
        LOG.debug("Created DuckDB dataset: {}", dataset);
        return new DuckDBDatasource(executor, dataset);
    }

    private static DataSource createSQLDataSource(SQLConfiguration config) {
        JDBCConnection jdbcConnection = new JDBCConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        jdbcConnection.connect();
        SQLQueryExecutor executor = new SQLQueryExecutor(jdbcConnection);

        // Optional Postgres auto-load. Triggered only when --csv is supplied; otherwise
        // we hit the existing remote-table path unchanged (e.g. pulsar). The CSV is
        // streamed from the client over JDBC via CopyManager, so the same code path
        // works against both the local Docker container (with /data mount) and the
        // remote pulsar server (which has no shared filesystem with this process).
        if (config.getCsvPath() != null) {
            ensurePostgresTableLoaded(executor, config);
        }

        SQLDataset dataset;

        // Check if dataset info exists in cache
        if (DatasetCache.hasDataset("sql", config.getSchemaName(), config.getTableName())) {
            dataset = (SQLDataset) DatasetCache.getDataset("sql", config.getSchemaName(), config.getTableName());
            dataset.setTimeFormat(config.getTimeFormat());
            dataset.setIdColumn(config.getIdColumn());
            dataset.setValueColumn(config.getValueColumn());
            dataset.setTimestampColumn(config.getTimestampColumn());
        } else {
            dataset = new SQLDataset(
                config.getSchemaName(),
                config.getTableName(),
                config.getTimestampColumn(),
                config.getIdColumn(),
                config.getValueColumn(),
                config.getTimeFormat()
            );
            fillSQLDatasetInfo(dataset, executor);
            DatasetCache.saveDataset("sql", dataset);
        }
        LOG.debug("Created SQL dataset: {}", dataset);
        return new SQLDatasource(executor, dataset);
    }

    private static DataSource createTrinoDataSource(TrinoConfiguration config) {
        JDBCConnection jdbcConnection = new JDBCConnection(
            config.getUrl(),
            null,
            null
        );
        jdbcConnection.connect();
        SQLQueryExecutor executor = new SQLQueryExecutor(jdbcConnection);

        SQLDataset dataset;

        // Check if dataset info exists in cache
        if (DatasetCache.hasDataset("trino", config.getSchemaName(), config.getTableName())) {
            dataset = (SQLDataset) DatasetCache.getDataset("trino", config.getSchemaName(), config.getTableName());
            dataset.setTimeFormat(config.getTimeFormat());
            dataset.setIdColumn(config.getIdColumn());
            dataset.setValueColumn(config.getValueColumn());
            dataset.setTimestampColumn(config.getTimestampColumn());
        } else {
            dataset = new SQLDataset(
                config.getSchemaName(),
                config.getTableName(),
                config.getTimestampColumn(),
                config.getIdColumn(),
                config.getValueColumn(),
                config.getTimeFormat()
            );
            fillTrinoDatasetInfo(dataset, executor);
            DatasetCache.saveDataset("trino", dataset);
        }
        LOG.debug("Created Trino dataset: {}", dataset);
        return new TrinoDatasource(executor, dataset);
    }

    /**
     * Bootstrap the Postgres table from a CSV if it doesn't already exist (or exists but is empty).
     * Schema is fixed to the (timestamp, id, value) layout used by every other dataset path.
     * The CSV is streamed from the client over JDBC via {@link CopyManager}, so it works
     * against both local Docker and the remote pulsar Postgres (no shared filesystem needed).
     */
    private static void ensurePostgresTableLoaded(SQLQueryExecutor executor, SQLConfiguration config) {
        String schema = config.getSchemaName() == null ? "public" : config.getSchemaName();
        String table = config.getTableName();
        String qualified = schema + "." + table;

        boolean needsLoad;
        try (ResultSet rs = executor.executeDbQuery(
                "SELECT to_regclass('" + qualified.replace("'", "''") + "') IS NOT NULL AS exists")) {
            rs.next();
            boolean exists = rs.getBoolean("exists");
            if (!exists) {
                needsLoad = true;
            } else {
                try (ResultSet rs2 = executor.executeDbQuery(
                        "SELECT EXISTS (SELECT 1 FROM " + qualified + " LIMIT 1) AS has_rows")) {
                    rs2.next();
                    needsLoad = !rs2.getBoolean("has_rows");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to probe Postgres table " + qualified, e);
        }

        if (!needsLoad) {
            LOG.info("Postgres table {} already populated, skipping CSV load", qualified);
            return;
        }

        java.io.File csvFile = new java.io.File(config.getCsvPath());
        if (!csvFile.isFile()) {
            throw new RuntimeException("CSV not found for bootstrap: " + csvFile.getAbsolutePath());
        }
        String tsCol = config.getTimestampColumn();
        String idCol = config.getIdColumn();
        String valCol = config.getValueColumn();

        LOG.info("Bootstrapping {} table {} from {}",
                config.isHypertable() ? "Timescale" : "Postgres", qualified, csvFile.getAbsolutePath());
        executor.executeStatement("CREATE SCHEMA IF NOT EXISTS " + schema);
        if (config.isHypertable()) {
            executor.executeStatement("CREATE EXTENSION IF NOT EXISTS timescaledb");
        }
        executor.executeStatement(
            "CREATE TABLE IF NOT EXISTS " + qualified + " ("
            + tsCol + " TIMESTAMP NOT NULL, "
            + idCol + " TEXT NOT NULL, "
            + valCol + " DOUBLE PRECISION NOT NULL)");
        // Convert to a hypertable on the empty table so COPY routes rows to chunks
        // directly (no migrate_data pass). Chunk interval is coarse on purpose: the
        // workload is large-range analytical scans, so fewer/bigger chunks beat the
        // per-chunk planning overhead of the 7-day default.
        if (config.isHypertable()) {
            executor.executeStatement(
                "SELECT create_hypertable('" + qualified.replace("'", "''") + "', '" + tsCol + "', "
                + "chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE)");
        }
        copyCsvIntoTable(executor.getJdbcConnection(), qualified, tsCol, idCol, valCol, csvFile);
        executor.executeStatement(
            "CREATE INDEX IF NOT EXISTS " + table + "_" + idCol + "_" + tsCol + "_idx "
            + "ON " + qualified + " (" + idCol + ", " + tsCol + ")");
        // Columnar compression is what makes Timescale a non-strawman scan baseline:
        // segment by sensor id, order by time — matching the (id, time-range) access
        // pattern of every aggregate query. Compress all chunks now so the benchmark
        // measures the compressed state rather than waiting on a background policy.
        if (config.isHypertable()) {
            executor.executeStatement(
                "ALTER TABLE " + qualified + " SET ("
                + "timescaledb.compress, "
                + "timescaledb.compress_segmentby = '" + idCol + "', "
                + "timescaledb.compress_orderby = '" + tsCol + "')");
            executor.executeStatement(
                "SELECT compress_chunk(show_chunks('" + qualified.replace("'", "''") + "'))");
        }
        executor.executeStatement("ANALYZE " + qualified);
        LOG.info("{} table {} loaded", config.isHypertable() ? "Timescale" : "Postgres", qualified);
    }

    /**
     * Stream a local CSV into {@code qualified} via the Postgres {@code CopyManager}.
     * This is the client-side equivalent of {@code COPY ... FROM '<path>'} and works
     * against any reachable server (no shared filesystem required).
     */
    private static void copyCsvIntoTable(JDBCConnection jdbcConnection,
                                         String qualified,
                                         String tsCol, String idCol, String valCol,
                                         java.io.File csvFile) {
        String copySql = "COPY " + qualified + " (" + tsCol + ", " + idCol + ", " + valCol + ") "
                       + "FROM STDIN WITH (FORMAT csv, HEADER true)";
        try {
            Connection conn = jdbcConnection.getConnection();
            CopyManager copy = conn.unwrap(PGConnection.class).getCopyAPI();
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(csvFile))) {
                long rows = copy.copyIn(copySql, in);
                LOG.info("Streamed {} rows into {} from {}", rows, qualified, csvFile.getAbsolutePath());
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Failed to stream CSV " + csvFile + " into " + qualified, e);
        }
    }

    private static void fillSQLDatasetInfo(SQLDataset dataset, SQLQueryExecutor sqlQueryExecutor) {
        try {
            // Fetch first timestamp
            String firstQuery = "SELECT MIN(" + dataset.getTimestampColumn() + ") as first_time FROM " + dataset.getTableName();
            ResultSet firstResult = sqlQueryExecutor.executeDbQuery(firstQuery);
            firstResult.next();
            long firstTime = DateTimeUtil.toEpochMilliUtc(firstResult.getTimestamp("first_time"));
            firstResult.close();

            // Fetch last timestamp
            String lastQuery = "SELECT MAX(" + dataset.getTimestampColumn() + ") as last_time FROM " + dataset.getTableName();
            ResultSet lastResult = sqlQueryExecutor.executeDbQuery(lastQuery);
            lastResult.next();
            long lastTime = DateTimeUtil.toEpochMilliUtc(lastResult.getTimestamp("last_time"));
            lastResult.close();

            // Fetch the second timestamp to calculate the sampling interval
            String secondQuery = "SELECT " + dataset.getTimestampColumn() + " FROM " + dataset.getTableName() +
                               " ORDER BY " + dataset.getIdColumn() + ", " + dataset.getTimestampColumn() + " ASC LIMIT 2";

            ResultSet secondResult = sqlQueryExecutor.executeDbQuery(secondQuery);
            secondResult.next();
            long samplingInterval = 0;
            if (secondResult.next()) {
                long secondTime = DateTimeUtil.toEpochMilliUtc(secondResult.getTimestamp(dataset.getTimestampColumn()));
                samplingInterval = secondTime - firstTime;
            }
            secondResult.close();

            // Set sampling interval and time range
            dataset.setSamplingInterval(samplingInterval);
            dataset.setTimeRange(new TimeRange(firstTime, lastTime));

            // Populate header (distinct sensor/measure IDs). ORDER BY for stable indexing across DBs.
            String headerQuery = "SELECT DISTINCT " + dataset.getIdColumn() + " FROM " + dataset.getTableName()
                    + " ORDER BY " + dataset.getIdColumn();
            ResultSet headerResult = sqlQueryExecutor.executeDbQuery(headerQuery);
            List<String> headers = new ArrayList<>();
            while (headerResult.next()) {
                headers.add(headerResult.getString(dataset.getIdColumn()));
            }
            headerResult.close();

            dataset.setHeader(headers.toArray(new String[0]));

        } catch (SQLException e) {
            throw new RuntimeException("Failed to fill SQL dataset info", e);
        }
    }

    private static void fillTrinoDatasetInfo(SQLDataset dataset, SQLQueryExecutor trinoQueryExecutor) {
        try {
            // Fetch first timestamp using Trino-specific functions
            String firstQuery = "SELECT min(" + dataset.getTimestampColumn() + ") as first_time FROM " + dataset.getTableName();
            ResultSet firstResult = trinoQueryExecutor.executeDbQuery(firstQuery);
            firstResult.next();
            long firstTime = DateTimeUtil.toEpochMilliUtc(firstResult.getTimestamp("first_time"));
            firstResult.close();

            // Fetch last timestamp using Trino-specific functions
            String lastQuery = "SELECT max(" + dataset.getTimestampColumn() + ") as last_time FROM " + dataset.getTableName();
            ResultSet lastResult = trinoQueryExecutor.executeDbQuery(lastQuery);
            lastResult.next();
            long lastTime = DateTimeUtil.toEpochMilliUtc(lastResult.getTimestamp("last_time"));
            lastResult.close();

            // Fetch the second timestamp to calculate the sampling interval
            String secondQuery = "SELECT " + dataset.getTimestampColumn() + " FROM " + dataset.getTableName() +
                               " ORDER BY " + dataset.getIdColumn() + ", " + dataset.getTimestampColumn() + " ASC LIMIT 2";

            ResultSet secondResult = trinoQueryExecutor.executeDbQuery(secondQuery);
            secondResult.next();
            long samplingInterval = 0;
            if (secondResult.next()) {
                long secondTime = DateTimeUtil.toEpochMilliUtc(secondResult.getTimestamp(dataset.getTimestampColumn()));
                samplingInterval = secondTime - firstTime;
            }
            secondResult.close();

            // Set sampling interval and time range
            dataset.setSamplingInterval(samplingInterval);
            dataset.setTimeRange(new TimeRange(firstTime, lastTime));

            // Populate header (distinct sensor/measure IDs). ORDER BY for stable indexing across DBs.
            String headerQuery = "SELECT DISTINCT " + dataset.getIdColumn() + " FROM " + dataset.getTableName()
                    + " ORDER BY " + dataset.getIdColumn();
            ResultSet headerResult = trinoQueryExecutor.executeDbQuery(headerQuery);
            List<String> headers = new ArrayList<>();
            while (headerResult.next()) {
                headers.add(headerResult.getString(dataset.getIdColumn()));
            }
            headerResult.close();

            dataset.setHeader(headers.toArray(new String[0]));

        } catch (SQLException e) {
            throw new RuntimeException("Failed to fill Trino dataset info", e);
        }
    }
}
