package gr.imsi.athenarc.experiments;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic one-shot CSV → DuckDB long-format loader.
 *
 * <p>Pivots a wide CSV (one row per timestamp, N value columns) into a long
 * table with columns {@code (timestamp TIMESTAMP, id VARCHAR, value DOUBLE)},
 * which is the schema the pattern-cache harness expects. After running this,
 * the harness can be pointed at the table directly with
 * {@code -type duckdb -table <name>} (no {@code -csv} flag required).
 *
 * <p>Usage:
 * <pre>
 *   java -cp target/pattern-cache-1.0-SNAPSHOT.jar \
 *        gr.imsi.athenarc.experiments.LoadCsv \
 *        --csv path/to/data.csv \
 *        --table my_table \
 *        --datetime '&lt;col&gt;[:&lt;strptime-format&gt;]' \
 *        --measures '&lt;Col1&gt;=&lt;id1&gt;,&lt;Col2&gt;=&lt;id2&gt;,...' \
 *        [--duckdb /tmp/vasta.duckdb]
 * </pre>
 *
 * <p>Examples:
 * <pre>
 *   # SPX (OHLCV, dd/MM/yyyy HH:mm datetime):
 *   ... --csv /opt/exp-data/spx.csv --table spx \
 *       --datetime 'datetime:%d/%m/%Y %H:%M' \
 *       --measures 'Open=open,High=high,Low=low,Close=close'
 *
 *   # ISO datetime, single value column:
 *   ... --csv /opt/exp-data/foo.csv --table foo \
 *       --datetime 'ts' \
 *       --measures 'value=foo'
 * </pre>
 *
 * <p>If the CSV is already in long format ({@code timestamp,id,value}), use the
 * harness's existing {@code -csv} flag instead of this tool.
 */
public final class LoadCsv {

    public static void main(String[] args) throws Exception {
        String duckdbPath = "/tmp/vasta.duckdb";
        String csvPath = null, table = null, datetimeArg = null, measuresArg = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--duckdb":   duckdbPath  = args[++i]; break;
                case "--csv":      csvPath     = args[++i]; break;
                case "--table":    table       = args[++i]; break;
                case "--datetime": datetimeArg = args[++i]; break;
                case "--measures": measuresArg = args[++i]; break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(2);
            }
        }
        csvPath     = require(csvPath, "--csv");
        table       = require(table, "--table");
        datetimeArg = require(datetimeArg, "--datetime");
        measuresArg = require(measuresArg, "--measures");

        // Parse --datetime <col>[:<format>]
        String dtCol;
        String dtFormat;
        int colon = datetimeArg.indexOf(':');
        if (colon < 0) {
            dtCol = datetimeArg.trim();
            dtFormat = null;
        } else {
            dtCol = datetimeArg.substring(0, colon).trim();
            dtFormat = datetimeArg.substring(colon + 1);
        }

        // Parse --measures Col=id,Col2=id2,...
        List<String[]> measures = new ArrayList<>();
        for (String pair : measuresArg.split("\\s*,\\s*")) {
            String[] kv = pair.split("=", 2);
            if (kv.length != 2) throw new IllegalArgumentException("Bad measure mapping: '" + pair + "' (expected Col=id)");
            measures.add(new String[]{kv[0].trim(), kv[1].trim()});
        }
        if (measures.isEmpty()) throw new IllegalArgumentException("--measures must have at least one entry");

        String escapedCsv = csvPath.replace("'", "''");
        String tsExpr;
        if (dtFormat == null) {
            tsExpr = quoteIdent(dtCol);
        } else {
            String escapedFmt = dtFormat.replace("'", "''");
            tsExpr = "strptime(CAST(" + quoteIdent(dtCol) + " AS VARCHAR), '" + escapedFmt + "')";
        }

        StringBuilder unionAll = new StringBuilder();
        for (int i = 0; i < measures.size(); i++) {
            String col = measures.get(i)[0];
            String id  = measures.get(i)[1].replace("'", "''");
            if (i > 0) unionAll.append("\nUNION ALL\n");
            unionAll.append("SELECT ").append(tsExpr).append(" AS timestamp, '")
                    .append(id).append("' AS id, ").append(quoteIdent(col)).append(" AS value FROM raw");
        }

        String ddl = "CREATE OR REPLACE TABLE " + quoteIdent(table) + " AS WITH raw AS (\n"
                + "  SELECT * FROM read_csv_auto('" + escapedCsv + "', header=true)\n"
                + ")\n" + unionAll;

        String url = "jdbc:duckdb:" + duckdbPath;
        System.out.println("Connecting to " + url);
        try (Connection c = DriverManager.getConnection(url);
             Statement st = c.createStatement()) {

            System.out.println("Loading " + csvPath + " into table '" + table + "' …");
            long t0 = System.nanoTime();
            st.execute(ddl);
            double elapsed = (System.nanoTime() - t0) / 1e9;
            System.out.printf("CREATE TABLE took %.2f s%n", elapsed);

            System.out.println("=== row count by id ===");
            try (ResultSet rs = st.executeQuery("SELECT id, COUNT(*) FROM " + quoteIdent(table) + " GROUP BY id ORDER BY id")) {
                while (rs.next()) System.out.printf("  %-15s %,d%n", rs.getString(1), rs.getLong(2));
            }

            System.out.println("=== timestamp range ===");
            try (ResultSet rs = st.executeQuery(
                    "SELECT MIN(timestamp), MAX(timestamp), COUNT(DISTINCT timestamp) FROM " + quoteIdent(table))) {
                if (rs.next()) {
                    System.out.println("  min=" + rs.getTimestamp(1)
                            + "  max=" + rs.getTimestamp(2)
                            + "  unique_ts=" + rs.getLong(3));
                }
            }
        }

        // Invalidate the on-disk dataset-info cache so the harness re-runs
        // sampling-interval / header detection on the new table.
        File cacheFile = new File("dataset_cache/duckdb__" + table + ".json");
        if (cacheFile.exists() && cacheFile.delete()) {
            System.out.println("Removed stale " + cacheFile);
        }

        System.out.println();
        System.out.println("Next: run the harness with");
        System.out.println("  java -jar target/pattern-cache-1.0-SNAPSHOT.jar \\");
        System.out.println("    -type duckdb -table " + table + " \\");
        System.out.println("    -mode timeCacheQueries -method approxOls \\");
        System.out.println("    -queries <your-queries.txt> -out output/" + table + " -runs 1");
    }

    private static String require(String value, String flag) {
        if (value == null || value.isEmpty()) {
            System.err.println("Missing required flag " + flag);
            System.exit(2);
        }
        return value;
    }

    /** Minimal, defensive quoting for SQL identifiers (column / table names). */
    private static String quoteIdent(String name) {
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }
        return name;
    }
}
