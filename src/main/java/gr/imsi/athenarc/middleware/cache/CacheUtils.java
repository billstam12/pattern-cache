package gr.imsi.athenarc.middleware.cache;

import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.TimeInterval;

public class CacheUtils {
    
     public static Map<Integer, List<TimeSeriesSpan>> fetchTimeSeriesSpans(DataSource dataSource, 
            long from, long to, 
            Map<Integer, List<TimeInterval>> alignedIntervalsPerMeasure, 
            Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure, String method) {

        // Fetch missing data
        AggregatedDataPoints newDataPoints = null;
                        
        // Create spans and add to cache
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = null;

        if(method.equalsIgnoreCase("m4")) {
            newDataPoints = dataSource.getM4DataPoints(from, to, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);
            return TimeSeriesSpanFactory.createM4Aggregate(newDataPoints, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);
        }
        else if(method.equalsIgnoreCase("m4Inf")) {
            newDataPoints = dataSource.getAggregatedDataPoints(from, to, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, AggregationFunctionsConfig.getAggregateFunctions(method));
            timeSeriesSpans = TimeSeriesSpanFactory.createM4InfAggregate(newDataPoints, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);
        } else if(method.equalsIgnoreCase("minmax")){
            newDataPoints = dataSource.getAggregatedDataPoints(from, to, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure, AggregationFunctionsConfig.getAggregateFunctions(method));
            timeSeriesSpans = TimeSeriesSpanFactory.createMinMaxAggregate(newDataPoints, alignedIntervalsPerMeasure, aggregateIntervalsPerMeasure);
        } else {
            throw new IllegalArgumentException("Unsupported method for fetching data: " + method);
        }
        return timeSeriesSpans;
    }
    
}
