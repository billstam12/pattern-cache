package gr.imsi.athenarc.visual.middleware.manager;

import gr.imsi.athenarc.visual.middleware.cache.TimeSeriesCache;
import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.manager.pattern.PatternQueryManager;
import gr.imsi.athenarc.visual.middleware.manager.visual.VisualizationQueryManager;
import gr.imsi.athenarc.visual.middleware.query.Query;
import gr.imsi.athenarc.visual.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.visual.middleware.query.visualization.VisualizationQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Query manager that handles different types of queries and uses a unified cache.
 */
public class QueryManager {
    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);
    
    private final TimeSeriesCache cache;
    private final PatternQueryManager patternQueryManager;
    private final VisualizationQueryManager visualizationQueryManager;

    public QueryManager(DataSource dataSource) {
        this.cache = new TimeSeriesCache();
        this.patternQueryManager = new PatternQueryManager(dataSource, cache);
        this.visualizationQueryManager = new VisualizationQueryManager(dataSource, cache);
    }
    
    /**
     * Execute a query based on its type.
     *
     * @param query the query to execute
     * @return the result of the query execution
     */
    public Object executeQuery(Query query) {
        LOG.info("Executing {} query from {} to {}", 
                query.getType(), query.getFrom(), query.getTo());
        
        switch (query.getType()) {
            case PATTERN:
                return executePatternQuery((PatternQuery) query);
            case VISUALIZATION:
                return executeVisualizationQuery((VisualizationQuery) query);
            default:
                throw new UnsupportedOperationException("Unsupported query type: " + query.getType());
        }
    }
    
    /**
     * Execute a pattern matching query.
     *
     * @param query the pattern query
     * @return the result of pattern matching
     */
    private Object executePatternQuery(PatternQuery query) {
        LOG.debug("Handling pattern query for measure {}", query.getMeasures());
        return patternQueryManager.executeQuery(query);
    }
    
    /**
     * Execute a visualization query for multiple measures.
     *
     * @param query the visualization query
     * @return map of measure IDs to their aggregated data points
     */
    private Map<Integer, AggregatedDataPoints> executeVisualizationQuery(VisualizationQuery query) {
        LOG.debug("Handling visualization query for {} measures", query.getMeasures().length);
    
        return visualizationQueryManager.executeQuery(query);
    }
    
    /**
     * Clear all cached data.
     */
    public void clearCache() {
        cache.clearCache();
    }
}
