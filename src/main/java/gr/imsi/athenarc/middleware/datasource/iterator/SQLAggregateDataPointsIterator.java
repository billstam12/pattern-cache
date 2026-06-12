package gr.imsi.athenarc.middleware.datasource.iterator;

import gr.imsi.athenarc.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.Stats;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;

/**
 * Flyweight iterator over the aggregate result set: {@link #next()} returns
 * {@code this} with the current row's fields refreshed in place, and
 * {@link #getStats()} likewise returns {@code this}. This is safe because the
 * sole consumer ({@code TimeSeriesSpanFactory.createAggregate}) drains each
 * point into a span before advancing the iterator.
 *
 * <p>Column presence is resolved once from {@link ResultSetMetaData} in the
 * constructor, so per-row reads never probe for absent columns — the old
 * per-row, per-column {@code try/catch} on a missing-column {@link SQLException}
 * is gone.
 */
public class SQLAggregateDataPointsIterator extends SQLIterator<AggregatedDataPoint>
        implements AggregatedDataPoint, Stats {

    private final Map<String, Integer> measuresMap;
    private final Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure;

    private final boolean hasMin;
    private final boolean hasMax;
    private final boolean hasFirst;
    private final boolean hasLast;
    private final boolean hasCount;
    private final boolean hasSum;

    // Current-row state (flyweight).
    private long from;
    private long to;
    private int measure;
    // NaN marks an absent or SQL-NULL value, matching AggregateStats' null semantics.
    private double minValue;
    private double maxValue;
    private double firstValue;
    private double lastValue;
    private double sumValue;
    // -1 marks "unset", same sentinel AggregateStats uses for the count fallback.
    private int count;

    public SQLAggregateDataPointsIterator(ResultSet resultSet, Map<String, Integer> measuresMap,
                                         Map<Integer, AggregateInterval> aggregateIntervalsPerMeasure) {
        super(resultSet);
        this.measuresMap = measuresMap;
        this.aggregateIntervalsPerMeasure = aggregateIntervalsPerMeasure;

        boolean min = false, max = false, first = false, last = false, cnt = false, sum = false;
        try {
            ResultSetMetaData md = resultSet.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                switch (md.getColumnLabel(i).toLowerCase(Locale.ROOT)) {
                    case "min":   min = true;   break;
                    case "max":   max = true;   break;
                    case "first": first = true; break;
                    case "last":  last = true;  break;
                    case "count": cnt = true;   break;
                    case "sum":   sum = true;   break;
                    default: break;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read result set metadata", e);
        }
        this.hasMin = min;
        this.hasMax = max;
        this.hasFirst = first;
        this.hasLast = last;
        this.hasCount = cnt;
        this.hasSum = sum;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        try {
            long timeBucket = getTimestampFromResultSet("time_bucket");
            String measureName = getSafeStringValue("measure_name");

            if (measureName == null || !measuresMap.containsKey(measureName)) {
                LOG.warn("Invalid measure field in record: {}", measureName);
                if (hasNext()) {
                    return next();
                }
                throw new RuntimeException("No valid data found");
            }

            int measureIndex = measuresMap.get(measureName);
            AggregateInterval aggregateInterval = aggregateIntervalsPerMeasure.get(measureIndex);
            long intervalMillis = aggregateInterval.getMultiplier() * getChronoUnitMillis(aggregateInterval.getChronoUnit());

            this.from = timeBucket;
            this.to = timeBucket + intervalMillis;
            this.measure = measureIndex;
            this.minValue = hasMin ? readDouble("min") : Double.NaN;
            this.maxValue = hasMax ? readDouble("max") : Double.NaN;
            this.firstValue = hasFirst ? readDouble("first") : Double.NaN;
            this.lastValue = hasLast ? readDouble("last") : Double.NaN;
            this.sumValue = hasSum ? readDouble("sum") : Double.NaN;
            this.count = hasCount ? readCount("count") : -1;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Created aggregate DataPoint for measure {}: min={}, max={}, first={}, last={} in bucket [{} - {}]",
                        measureName, minValue, maxValue, firstValue, lastValue, from, to);
            }
            return this;
        } catch (SQLException e) {
            LOG.error("Error reading result set", e);
            throw new RuntimeException("Error reading result set", e);
        }
    }

    private double readDouble(String columnLabel) throws SQLException {
        double value = resultSet.getDouble(columnLabel);
        return resultSet.wasNull() ? Double.NaN : value;
    }

    private int readCount(String columnLabel) throws SQLException {
        int value = resultSet.getInt(columnLabel);
        return resultSet.wasNull() ? -1 : value;
    }

    private long getChronoUnitMillis(ChronoUnit unit) {
        switch (unit) {
            case MILLIS:
                return 1;
            case SECONDS:
                return 1000;
            case MINUTES:
                return 60 * 1000;
            case HOURS:
                return 60 * 60 * 1000;
            case DAYS:
                return 24 * 60 * 60 * 1000;
            case WEEKS:
                return 7 * 24 * 60 * 60 * 1000;
            case MONTHS:
                return 30L * 24L * 60L * 60L * 1000L; // Approximate
            case YEARS:
                return 365L * 24L * 60L * 60L * 1000L; // Approximate
            default:
                throw new IllegalArgumentException("Unsupported ChronoUnit: " + unit);
        }
    }

    // --- AggregatedDataPoint (flyweight: returns current-row state) ---

    @Override
    public long getTimestamp() {
        return from;
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
    public double getValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMeasure() {
        return measure;
    }

    @Override
    public Stats getStats() {
        return this;
    }

    // --- Stats (flyweight: returns current-row state) ---

    @Override
    public int getCount() {
        if (count >= 0) {
            return count;
        }
        // Legacy fallback, mirrors AggregateStats: a non-NaN-fields fingerprint
        // so callers using getCount() > 0 as a "has any data" sentinel keep working.
        int fingerprint = 0;
        if (!Double.isNaN(firstValue)) fingerprint++;
        if (!Double.isNaN(lastValue)) fingerprint++;
        if (!Double.isNaN(minValue)) fingerprint++;
        if (!Double.isNaN(maxValue)) fingerprint++;
        return fingerprint;
    }

    @Override
    public double getMinValue() {
        if (Double.isNaN(minValue)) {
            throw new IllegalStateException("minValue is not set");
        }
        return minValue;
    }

    @Override
    public double getMaxValue() {
        if (Double.isNaN(maxValue)) {
            throw new IllegalStateException("maxValue is not set");
        }
        return maxValue;
    }

    @Override
    public double getFirstValue() {
        if (Double.isNaN(firstValue)) {
            throw new IllegalStateException("firstValue is not set");
        }
        return firstValue;
    }

    @Override
    public double getLastValue() {
        if (Double.isNaN(lastValue)) {
            throw new IllegalStateException("lastValue is not set");
        }
        return lastValue;
    }

    @Override
    public double getSum() {
        if (Double.isNaN(sumValue)) {
            throw new IllegalStateException("sumValue is not set");
        }
        return sumValue;
    }

    @Override
    public long getMinTimestamp() {
        return (from + to) / 2;
    }

    @Override
    public long getMaxTimestamp() {
        return (from + to) / 2;
    }

    @Override
    public long getFirstTimestamp() {
        return from + 1;
    }

    @Override
    public long getLastTimestamp() {
        return to - 1;
    }
}
