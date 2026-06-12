package gr.imsi.athenarc.experiments.util;

import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.SlopeFunction;
import gr.imsi.athenarc.middleware.query.Query;
import gr.imsi.athenarc.middleware.query.pattern.*;
import gr.imsi.athenarc.middleware.query.visual.VisualQueryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads a query sequence saved by {@code scripts/queries/generate_queries.py}. The
 * harness no longer generates sequences in Java — Markov-walk generation lives
 * in Python. This class only consumes the on-disk format.
 *
 * <p>Line schema:
 * <pre>
 *   from_ms,to_ms,from_str,to_str,measure,width,height,accuracy,type,label[,...]
 * </pre>
 * <ul>
 *   <li>{@code type=visual}  — {@code label ∈ {initial, pan, zoomIn, zoomOut, resize, measureChange}}</li>
 *   <li>{@code type=pattern} — {@code label=custom, time_unit, segment_spec}
 *       (e.g. {@code custom,1 Minutes,5:-80:-40;5:40:80}) OR
 *       {@code label=<predefined-name>, time_unit} (e.g. {@code v_shape_small,1 Minutes}).</li>
 * </ul>
 */
public final class QueryFileLoader {

    private static final Logger LOG = LoggerFactory.getLogger(QueryFileLoader.class);

    static {
        // Predefined-pattern table is needed to resolve names like `v_shape_small`.
        // Idempotent — safe to call repeatedly.
        PredefinedPattern.initializePredefinedPatterns();
    }

    private QueryFileLoader() {}

    public static List<TypedQuery> load(String filePath) {
        List<TypedQuery> queries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                TypedQuery q = parseQueryFromLine(line);
                if (q != null) queries.add(q);
            }
            LOG.info("Loaded {} queries from {}", queries.size(), filePath);
        } catch (IOException e) {
            throw new RuntimeException("Error reading queries from " + filePath, e);
        }
        return queries;
    }

    private static TypedQuery parseQueryFromLine(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length < 8) {
                LOG.warn("Invalid line format (too few fields): {}", line);
                return null;
            }

            long from = Long.parseLong(parts[0]);
            long to = Long.parseLong(parts[1]);

            // Multi-measure: "1|2|9" — first measure is canonical for pattern queries.
            String[] measureStrings = parts[4].split("\\|");
            List<Integer> measures = new ArrayList<>();
            for (String measureStr : measureStrings) {
                measures.add(Integer.parseInt(measureStr));
            }

            int viewportWidth = Integer.parseInt(parts[5]);
            int viewportHeight = Integer.parseInt(parts[6]);
            double accuracy = Double.parseDouble(parts[7]);

            boolean isPattern = "pattern".equals(parts[8]);
            String opTypeStr = parts[9];
            UserOpType opType = parseUserOpType(opTypeStr);
            int patternId = -1;

            Query query;
            if (isPattern) {
                String[] aggregateInterval = parts[10].split(" ");
                AggregateInterval timeUnit = AggregateInterval.of(
                        Long.parseLong(aggregateInterval[0]),
                        ChronoUnit.valueOf(aggregateInterval[1].toUpperCase()));

                List<PatternNode> patternNodes;
                if ("custom".equals(opTypeStr)) {
                    if (parts.length < 12) {
                        LOG.warn("Custom pattern line missing segment spec at index 11: {}", line);
                        return null;
                    }
                    patternNodes = parseCustomPatternSpec(parts[11]);
                } else {
                    patternId = getPatternIdByName(opTypeStr);
                    PredefinedPattern predefinedPattern = PredefinedPattern.getPatternById(patternId);
                    if (predefinedPattern == null) {
                        LOG.warn("Unknown pattern name '{}' on line: {}", opTypeStr, line);
                        return null;
                    }
                    patternNodes = predefinedPattern.getPatternNodes();
                }

                query = new PatternQueryBuilder()
                        .withTimeRange(from, to)
                        .withMeasure(measures.get(0))
                        .withTimeUnit(timeUnit)
                        .withSlopeFunction(SlopeFunction.FIRST_LAST)
                        .withPatternNodes(patternNodes)
                        .withViewPort(viewportWidth, viewportHeight)
                        .withAccuracy(accuracy)
                        .build();
            } else {
                query = new VisualQueryBuilder()
                        .withTimeRange(from, to)
                        .withMeasures(measures)
                        .withViewPort(viewportWidth, viewportHeight)
                        .withAccuracy(accuracy)
                        .build();
            }
            return new TypedQuery(query, opType, patternId);
        } catch (Exception e) {
            LOG.error("Error parsing query line: " + line, e);
            return null;
        }
    }

    private static UserOpType parseUserOpType(String opTypeStr) {
        if ("initial".equals(opTypeStr)) return null;
        switch (opTypeStr) {
            case "pan":            return UserOpType.P;
            case "zoomIn":         return UserOpType.ZI;
            case "zoomOut":        return UserOpType.ZO;
            case "resize":         return UserOpType.R;
            case "measureChange":  return UserOpType.MC;
            case "custom":         return UserOpType.PD;
            default:
                if (getPatternIdByName(opTypeStr) != -1) return UserOpType.PD;
                LOG.warn("Unknown operation type: {}", opTypeStr);
                return null;
        }
    }

    /**
     * Parse a custom inline pattern spec of the form
     * {@code len:minDeg:maxDeg[;len:minDeg:maxDeg ...]}. Each segment is exactly
     * {@code len} buckets long with angle constrained to {@code [minDeg, maxDeg]}.
     * Use {@code len:*:*} to accept any angle.
     */
    private static List<PatternNode> parseCustomPatternSpec(String spec) {
        List<PatternNode> nodes = new ArrayList<>();
        for (String seg : spec.split(";")) {
            seg = seg.trim();
            if (seg.isEmpty()) continue;
            String[] f = seg.split(":");
            if (f.length != 3) {
                throw new IllegalArgumentException(
                        "Bad custom pattern segment '" + seg + "', expected 'len:minDeg:maxDeg'");
            }
            int len = Integer.parseInt(f[0].trim());
            ValueFilter vf;
            if ("*".equals(f[1].trim()) || "*".equals(f[2].trim())) {
                vf = ValueFilter.any();
            } else {
                vf = ValueFilter.custom(Float.parseFloat(f[1].trim()), Float.parseFloat(f[2].trim()));
            }
            nodes.add(new SingleNode(
                    new SegmentSpecification(new TimeFilter(false, len, len), vf),
                    RepetitionFactor.exactly(1)));
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Empty custom pattern spec: '" + spec + "'");
        }
        return nodes;
    }

    private static int getPatternIdByName(String patternName) {
        for (Integer id : PredefinedPattern.getAllPatternIds()) {
            PredefinedPattern p = PredefinedPattern.getPatternById(id);
            if (p.getName().equalsIgnoreCase(patternName)) return id;
        }
        return -1;
    }
}
