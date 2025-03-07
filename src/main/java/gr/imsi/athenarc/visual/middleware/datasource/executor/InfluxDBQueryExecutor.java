package gr.imsi.athenarc.visual.middleware.datasource.executor;

import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connection.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBQueryExecutor.class);

    private final InfluxDBConnection databaseConnection;
    private final InfluxDBDataset dataset;


    public InfluxDBQueryExecutor(DatabaseConnection databaseConnection, AbstractDataset dataset) {
        this.databaseConnection = (InfluxDBConnection) databaseConnection;
        this.dataset = (InfluxDBDataset) dataset;
    }


    @Override
    public Map<Integer, List<DataPoint>> executeM4Query(DataSourceQuery q) {
        return collect(executeM4InfluxQuery((InfluxDBQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeRawQuery(DataSourceQuery q) {
        return collect(executeRawInfluxQuery((InfluxDBQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeMinMaxQuery(DataSourceQuery q) {return collect(executeMinMaxInfluxQuery((InfluxDBQuery) q));}

    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1 == null && s2 == null) return 0;//swapping has no point here
            if (s1 == null) return 1;
            if (s2 == null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };


    public List<FluxTable> executeM4InfluxQuery(InfluxDBQuery q) {
        String flux = q.m4QuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeMinMaxInfluxQuery(InfluxDBQuery q) {
        String flux = q.minMaxQuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeRawInfluxQuery(InfluxDBQuery q){
        String flux = q.rawQuerySkeleton();
        return executeDbQuery(flux);
    }

    private Map<Integer, List<DataPoint>> collect(List<FluxTable> tables) {
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Integer fieldId = Arrays.asList(dataset.getHeader()).indexOf(fluxRecord.getField());
                data.computeIfAbsent(fieldId, k -> new ArrayList<>()).add(
                        new ImmutableDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(),
                                Double.parseDouble(Objects.requireNonNull(fluxRecord.getValue()).toString()), fieldId));
            }
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        return data;
    }

    public List<FluxTable> executeDbQuery(String query) {
        QueryApi queryApi = databaseConnection.getClient().getQueryApi();
        LOG.info("Executing Query: \n" + query);
        return queryApi.query(query);
    }


    public Map<Integer, List<DataPoint>> execute(String query) {
        return collect(executeDbQuery(query));
    }
}