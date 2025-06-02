package gr.imsi.athenarc.middleware.pattern;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;

public class PatternQueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;

    private static final Set<String> AGGREGATE_FUNCTIONS = AggregationFunctionsConfig.getDefaultAggregateFunctions();

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache) {
        this.dataSource = dataSource;
        this.cache = cache;
    }

    public PatternQueryResults executeQuery(PatternQuery query) {
        return PatternUtils.executePatternQueryWithCache(query, dataSource, cache, AGGREGATE_FUNCTIONS);
    }
}
