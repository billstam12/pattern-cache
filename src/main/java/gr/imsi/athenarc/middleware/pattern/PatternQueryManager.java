package gr.imsi.athenarc.middleware.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;

public class PatternQueryManager {

    private static final Logger LOG = LoggerFactory.getLogger(PatternQueryManager.class);

    private final DataSource dataSource;
    private final TimeSeriesCache cache;
    private final String method;
    private final boolean adaptation;

    public PatternQueryManager(DataSource dataSource, TimeSeriesCache cache, String method, boolean adaptation) {
        this.dataSource = dataSource;
        this.cache = cache;
        this.method = method;
        this.adaptation = adaptation;
    }

    public PatternQueryResults executeQuery(PatternQuery query) {
        return PatternQueryExecutor.executePatternQueryWithCache(query, dataSource, cache, method, adaptation);
    }
}
