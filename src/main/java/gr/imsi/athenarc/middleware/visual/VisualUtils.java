package gr.imsi.athenarc.middleware.visual;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.crypto.Data;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.middleware.cache.M4AggregateTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.RawTimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpan;
import gr.imsi.athenarc.middleware.cache.TimeSeriesSpanFactory;
import gr.imsi.athenarc.middleware.config.AggregationFunctionsConfig;
import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.middleware.domain.DataPoint;
import gr.imsi.athenarc.middleware.domain.DataPoints;
import gr.imsi.athenarc.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.middleware.domain.TimeInterval;
import gr.imsi.athenarc.middleware.domain.TimeRange;
import gr.imsi.athenarc.middleware.query.visual.VisualQuery;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryResults;

public class VisualUtils {


    public static VisualQueryResults executeRawQuery(VisualQuery query, DataSource dataSource){
        VisualQueryResults queryResults = new VisualQueryResults();
        
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, List<DataPoint>> rawData = new HashMap<>();
        for(int measure : query.getMeasures()){
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getTo()));
            missingIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), missingIntervalsPerMeasure);
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createRaw(dataPoints, missingIntervalsPerMeasure);
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPointsForMeasure = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                Iterator<DataPoint> it = ((RawTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    DataPoint dataPoint = it.next();
                    dataPointsForMeasure.add(dataPoint);

                }
            }
            rawData.put(measure, dataPointsForMeasure);
        }
        queryResults.setData(rawData);
        return queryResults;
    }
    public static VisualQueryResults executeM4Query(VisualQuery query, DataSource dataSource) {
        VisualQueryResults queryResults = new VisualQueryResults();
        Map<Integer, List<DataPoint>> m4Data = new HashMap<>();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, AggregateInterval> aggregateIntervals = new HashMap<>(query.getMeasures().size());

        long interval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
        AggregateInterval aggInterval = AggregateInterval.of(interval, ChronoUnit.MILLIS);
        long startPixelColumn = query.getFrom();
        long endPixelColumn = query.getFrom() + interval * (query.getViewPort().getWidth());

        for (Integer measure : query.getMeasures()) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getFrom() + interval * (query.getViewPort().getWidth())));
            missingIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            aggregateIntervals.put(measure, aggInterval);
        }

        AggregatedDataPoints missingDataPoints = 
            dataSource.getAggregatedDataPointsWithTimestamps(startPixelColumn, endPixelColumn, missingIntervalsPerMeasure, aggregateIntervals, AggregationFunctionsConfig.getAggregateFunctions("m4"));
        
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingIntervalsPerMeasure, aggregateIntervals, "m4");
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPoints = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                Iterator<AggregatedDataPoint> it = ((M4AggregateTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = it.next();
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getFirstDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getFirstDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMinDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMinDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMaxDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMaxDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getLastDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getLastDataPoint().getValue(), measure));

                }
            }
            m4Data.put(measure, dataPoints);
        }
        Map<Integer, ErrorResults> error = new HashMap<>();
        for(Integer m : query.getMeasures()){
            error.put(m, new ErrorResults());
        }
        queryResults.setData(m4Data);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        queryResults.setQueryTime(queryTime);
        return queryResults;
    }
}
