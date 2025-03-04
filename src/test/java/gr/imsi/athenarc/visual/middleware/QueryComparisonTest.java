package gr.imsi.athenarc.visual.middleware;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.util.QueryResultComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryComparisonTest {
//     private static final Logger LOG = LoggerFactory.getLogger(QueryComparisonTest.class);

//     private DataSource influxDB;
//     private DataSource postgreSQL;
//     private Map<String, List<TimeInterval>> missingIntervalsPerMeasure;
//     private Map<String, Integer> measuresMap;

//     @BeforeEach
//     void setUp() {
//         // Initialize your data sources with appropriate connection details
//         influxDB = new InfluxDBConfiguration(
//             "your_bucket",
//             "your_measurement",
//             "your_influx_url",
//             "your_token"
//         );

//         postgreSQL = new PostgreSQLDataSource(
//             "jdbc:postgresql://localhost:5432/your_db",
//             "your_username",
//             "your_password"
//         );

//         // Initialize test data
//         initializeTestData();
//     }

//     private void initializeTestData() {
//         // Setup measures map
//         measuresMap = new HashMap<>();
//         measuresMap.put("measure1", 1);
//         measuresMap.put("measure2", 2);

//         // Setup time intervals
//         missingIntervalsPerMeasure = new HashMap<>();
//         List<TimeInterval> intervals = Collections.singletonList(
//             new TimeRange(
//                 System.currentTimeMillis() - 3600000, // 1 hour ago
//                 System.currentTimeMillis()
//             )
//         );
//         missingIntervalsPerMeasure.put("measure1", intervals);
//         missingIntervalsPerMeasure.put("measure2", intervals);
//     }

//     @Test
//     void compareRawQueryResults() {
//         LOG.info("Starting raw query comparison test");

//         // Get iterators from both data sources
//         Iterator<DataPoint> influxResults = influxDB.getRawData(
//             missingIntervalsPerMeasure,
//             measuresMap
//         );

//         Iterator<DataPoint> postgresResults = postgreSQL.getRawData(
//             missingIntervalsPerMeasure,
//             measuresMap
//         );

//         // Compare results
//         boolean areEqual = QueryResultComparator.compareResults(influxResults, postgresResults);
//         assertTrue(areEqual, "Query results from InfluxDB and PostgreSQL should match");
//     }

//     @Test
//     void compareMinMaxQueryResults() {
//         LOG.info("Starting min-max query comparison test");

//         // Add aggregation intervals for min-max query
//         Map<String, Integer> aggregateIntervals = new HashMap<>();
//         aggregateIntervals.put("measure1", 60000); // 1 minute
//         aggregateIntervals.put("measure2", 60000);

//         Iterator<DataPoint> influxResults = influxDB.getMinMaxData(
//             missingIntervalsPerMeasure,
//             measuresMap,
//             aggregateIntervals
//         );

//         Iterator<DataPoint> postgresResults = postgreSQL.getMinMaxData(
//             missingIntervalsPerMeasure,
//             measuresMap,
//             aggregateIntervals
//         );

//         boolean areEqual = QueryResultComparator.compareResults(influxResults, postgresResults);
//         assertTrue(areEqual, "Min-max query results from InfluxDB and PostgreSQL should match");
//     }

//     @Test
//     void compareM4QueryResults() {
//         LOG.info("Starting M4 query comparison test");

//         Map<String, Integer> aggregateIntervals = new HashMap<>();
//         aggregateIntervals.put("measure1", 60000);
//         aggregateIntervals.put("measure2", 60000);

//         Iterator<DataPoint> influxResults = influxDB.getM4Data(
//             missingIntervalsPerMeasure,
//             measuresMap,
//             aggregateIntervals
//         );

//         Iterator<DataPoint> postgresResults = postgreSQL.getM4Data(
//             missingIntervalsPerMeasure,
//             measuresMap,
//             aggregateIntervals
//         );

//         boolean areEqual = QueryResultComparator.compareResults(influxResults, postgresResults);
//         assertTrue(areEqual, "M4 query results from InfluxDB and PostgreSQL should match");
//     }
}
