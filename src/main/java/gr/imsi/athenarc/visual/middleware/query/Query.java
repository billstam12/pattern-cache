package gr.imsi.athenarc.visual.middleware.query;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;

public class Query implements TimeInterval {

    long from;
    long to;
    List<Integer> measures;
    ViewPort viewPort;
    Map<Integer, Double[]> filter;
    float accuracy;

    public Query() {}
    public Query(long from, long to, List<Integer> measures, float accuracy, int width, int height, Map<Integer, Double[]> filter) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.filter = filter;
        this.accuracy = accuracy;
        this.viewPort = new ViewPort(width, height);
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public float getAccuracy() {
        return accuracy;
    }

    public Map<Integer, Double[]> getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "Query{" +
                "from=" + from +
                ", to=" + to +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                ", filter= " + filter +
                ", accuracy=" + accuracy +
                '}';
    }
}
