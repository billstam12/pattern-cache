package gr.imsi.athenarc.middleware.pattern;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;

/**
 * Executor for running SQL MATCH_RECOGNIZE queries for pattern matching.
 * Generates SQL queries that use MATCH_RECOGNIZE to find patterns based on
 * regression angles calculated using least squares.
 * This executor dynamically generates SQL based on the PatternQuery structure.
 */
public class MatchRecognizeQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MatchRecognizeQueryExecutor.class);

    /**
     * Execute a pattern query using SQL MATCH_RECOGNIZE approach.
     * This method generates and executes a SQL query with MATCH_RECOGNIZE
     * to find patterns based on regression angles extracted from PatternNodes.
     * 
     * @param query The pattern query to execute
     * @param dataSource The data source to execute the query against
     * @param method The method type (e.g., "firstLast", "ols", etc.)
     * @return Pattern query results containing the SQL query and execution details
     */
    public static PatternQueryResults executeMatchRecognizeQuery(PatternQuery query, DataSource dataSource, String method) {
        long startTime = System.currentTimeMillis();
        
        try {
            // Generate the SQL query
            String sqlQuery = generateMatchRecognizeSQL(query, dataSource);
            
            LOG.info("Generated MATCH_RECOGNIZE SQL query: {}", sqlQuery);
            
            // Create result with the generated SQL query
            PatternQueryResults results = new PatternQueryResults();
            results.setExecutionTime(System.currentTimeMillis() - startTime);
            
            LOG.info("MATCH_RECOGNIZE query generated in {} ms", results.getExecutionTime());
            
            return results;
            
        } catch (Exception e) {
            LOG.error("Failed to execute MATCH_RECOGNIZE query", e);
            throw new RuntimeException("Failed to execute MATCH_RECOGNIZE query", e);
        }
    }

    /**
     * Generate the SQL MATCH_RECOGNIZE query based on the pattern query parameters.
     * This method dynamically extracts pattern information from PatternNodes.
     * 
     * @param query The pattern query containing pattern nodes and constraints
     * @param dataSource The data source for table name and column information
     * @return The complete SQL query string
     */
    private static String generateMatchRecognizeSQL(PatternQuery query, DataSource dataSource) {
        StringBuilder sql = new StringBuilder();
        
        // Extract query parameters
        String tableName = dataSource.getDataset().getTableName();
        int measureId = query.getMeasures().get(0); // Assuming single measure for now
        AggregateInterval timeUnit = query.getTimeUnit();
        List<PatternNode> patternNodes = query.getPatternNodes();
        
        // Generate the bucketing interval
        String bucketingExpression = generateBucketingExpression(timeUnit);
        
        // Extract pattern segments and their constraints
        List<PatternSegment> segments = extractPatternSegments(patternNodes);
        
        // Build the main query
        sql.append("SELECT *\n");
        sql.append("FROM (\n");
        sql.append("  SELECT\n");
        sql.append("    ").append(bucketingExpression).append(" AS bucket_start,\n");
        sql.append("    SUM( to_unixtime(timestamp) ) AS sum_x,\n");
        sql.append("    SUM( value ) AS sum_y,\n");
        sql.append("    SUM( to_unixtime(timestamp) * value ) AS sum_xy,\n");
        sql.append("    SUM( to_unixtime(timestamp) * to_unixtime(timestamp) ) AS sum_x2,\n");
        sql.append("    COUNT(*) AS n\n");
        sql.append("  FROM ").append(tableName).append("\n");
        sql.append("  WHERE id = 'value_").append(measureId).append("'\n");
        
        // Add time range filter if specified
        if (query.getFrom() > 0 && query.getTo() > 0) {
            sql.append("    AND timestamp >= FROM_UNIXTIME(").append(query.getFrom() / 1000).append(")\n");
            sql.append("    AND timestamp <= FROM_UNIXTIME(").append(query.getTo() / 1000).append(")\n");
        }
        
        sql.append("  GROUP BY 1\n");
        sql.append(") bucketed_stats\n");
        sql.append("MATCH_RECOGNIZE (\n");
        sql.append("  ORDER BY bucket_start\n");
        sql.append("  MEASURES\n");
        
        // Generate measures for each pattern segment
        sql.append(generateMeasures(segments, timeUnit));
        
        sql.append("  ONE ROW PER MATCH\n");
        sql.append("  AFTER MATCH SKIP TO NEXT ROW\n");
        sql.append("  PATTERN (").append(generatePattern(segments)).append(")\n");
        sql.append("  DEFINE\n");
        sql.append(generateDefine(segments));
        sql.append(")\n");
        
        // Add WHERE clause for angle constraints
        String whereClause = generateWhereClause(segments);
        if (!whereClause.isEmpty()) {
            sql.append("WHERE\n").append(whereClause);
        }
        
        sql.append("ORDER BY seg1_start;");
        
        return sql.toString();
    }

    /**
     * Generate the appropriate SQL bucketing expression based on the time unit.
     * This handles different calendar intervals (seconds, minutes, hours, days, etc.)
     */
    private static String generateBucketingExpression(AggregateInterval timeUnit) {
        java.time.temporal.ChronoUnit unit = timeUnit.getChronoUnit();
        long multiplier = timeUnit.getMultiplier();
        
        switch (unit) {
            case MILLIS:
                // For milliseconds, use epoch milliseconds and round down
                return String.format("FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) * 1000 / %d) * %d / 1000)", 
                    multiplier, multiplier);
                    
            case SECONDS:
                // For seconds, use epoch seconds and round down
                return String.format("FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / %d) * %d)", 
                    multiplier, multiplier);
                    
            case MINUTES:
                // For minutes, truncate to hour and add minute intervals
                return String.format("date_trunc('hour', timestamp) + INTERVAL '1' minute * (FLOOR(EXTRACT(minute FROM timestamp) / %d) * %d)", 
                    multiplier, multiplier);
                    
            case HOURS:
                // For hours, truncate to day and add hour intervals
                return String.format("date_trunc('day', timestamp) + INTERVAL '1' hour * (FLOOR(EXTRACT(hour FROM timestamp) / %d) * %d)", 
                    multiplier, multiplier);
                    
            case DAYS:
                // For days, truncate to the appropriate day boundary
                if (multiplier == 1) {
                    return "date_trunc('day', timestamp)";
                } else {
                    return String.format("date_trunc('day', timestamp) + INTERVAL '1' day * (FLOOR(DATEDIFF(timestamp, '1970-01-01') / %d) * %d)", 
                        multiplier, multiplier);
                }
            case WEEKS:
                // For weeks, truncate to week boundary
                if (multiplier == 1) {
                    return "date_trunc('week', timestamp)";
                } else {
                    return String.format("date_trunc('week', timestamp) + INTERVAL '1' week * (FLOOR(WEEK(timestamp) / %d) * %d)", 
                        multiplier, multiplier);
                }
                
            case MONTHS:
                // For months, truncate to month boundary
                if (multiplier == 1) {
                    return "date_trunc('month', timestamp)";
                } else {
                    return String.format("date_trunc('month', timestamp) + INTERVAL '1' month * (FLOOR(MONTH(timestamp) / %d) * %d)", 
                        multiplier, multiplier);
                }
                
            case YEARS:
                // For years, truncate to year boundary
                if (multiplier == 1) {
                    return "date_trunc('year', timestamp)";
                } else {
                    return String.format("date_trunc('year', timestamp) + INTERVAL '1' year * (FLOOR(YEAR(timestamp) / %d) * %d)", 
                        multiplier, multiplier);
                }
                
            default:
                throw new IllegalArgumentException("Unsupported time unit for bucketing: " + unit);
        }
    }

    /**
     * Generate SQL INTERVAL expression for adding to timestamps based on the time unit.
     */
    private static String generateIntervalExpression(AggregateInterval timeUnit) {
        java.time.temporal.ChronoUnit unit = timeUnit.getChronoUnit();
        long multiplier = timeUnit.getMultiplier();
        
        switch (unit) {
            case MILLIS:
                return String.format("INTERVAL '%d' MILLISECOND", multiplier);
            case SECONDS:
                return String.format("INTERVAL '%d' SECOND", multiplier);
            case MINUTES:
                return String.format("INTERVAL '%d' MINUTE", multiplier);
            case HOURS:
                return String.format("INTERVAL '%d' HOUR", multiplier);
            case DAYS:
                return String.format("INTERVAL '%d' DAY", multiplier);
            case WEEKS:
                return String.format("INTERVAL '%d' WEEK", multiplier);
            case MONTHS:
                return String.format("INTERVAL '%d' MONTH", multiplier);
            case YEARS:
                return String.format("INTERVAL '%d' YEAR", multiplier);
            default:
                throw new IllegalArgumentException("Unsupported time unit for interval expression: " + unit);
        }
    }

    /**
     * Extract pattern segments from PatternNodes, flattening groups and handling repetitions.
     */
    private static List<PatternSegment> extractPatternSegments(List<PatternNode> patternNodes) {
        List<PatternSegment> segments = new ArrayList<>();
        int segmentIndex = 0;
        
        for (PatternNode node : patternNodes) {
            segments.addAll(extractSegmentsFromNode(node, segmentIndex));
            segmentIndex = segments.size();
        }
        
        return segments;
    }

    /**
     * Extract segments from a single PatternNode (SingleNode or GroupNode).
     */
    private static List<PatternSegment> extractSegmentsFromNode(PatternNode node, int startIndex) {
        List<PatternSegment> segments = new ArrayList<>();
        
        if (node instanceof SingleNode) {
            SingleNode singleNode = (SingleNode) node;
            SegmentSpecification spec = singleNode.getSpec();
            RepetitionFactor repetition = singleNode.getRepetitionFactor();
            
            // For SingleNode, create one segment with repetition info
            PatternSegment segment = new PatternSegment(
                startIndex,
                getSegmentVariable(startIndex),
                spec.getValueFilter(),
                spec.getTimeFilter(),
                repetition
            );
            segments.add(segment);
            
        } else if (node instanceof GroupNode) {
            GroupNode groupNode = (GroupNode) node;
            RepetitionFactor groupRepetition = groupNode.getRepetitionFactor();
            
            // For GroupNode, extract all children and apply group repetition
            List<PatternNode> children = groupNode.getChildren();
            int childIndex = startIndex;
            
            for (PatternNode child : children) {
                if (child instanceof SingleNode) {
                    SingleNode singleChild = (SingleNode) child;
                    SegmentSpecification spec = singleChild.getSpec();
                    
                    // Create segment with group repetition
                    PatternSegment segment = new PatternSegment(
                        childIndex,
                        getSegmentVariable(childIndex),
                        spec.getValueFilter(),
                        spec.getTimeFilter(),
                        groupRepetition // Use group's repetition for all children
                    );
                    segments.add(segment);
                    childIndex++;
                }
            }
        }
        
        return segments;
    }

    /**
     * Generate the MEASURES clause for calculating regression angles of each segment.
     */
    private static String generateMeasures(List<PatternSegment> segments, AggregateInterval timeUnit) {
        StringBuilder measures = new StringBuilder();
        
        for (int i = 0; i < segments.size(); i++) {
            PatternSegment segment = segments.get(i);
            String segmentVar = segment.variable;
            int segmentNum = i + 1;
            
            // Add regression angle calculation for this segment
            measures.append("    -- ").append(segmentVar).append(" segment regression angle\n");
            measures.append("    atan(\n");
            measures.append("      (\n");
            measures.append("        SUM(").append(segmentVar).append(".n) * SUM(").append(segmentVar).append(".sum_xy)\n");
            measures.append("        - SUM(").append(segmentVar).append(".sum_x) * SUM(").append(segmentVar).append(".sum_y)\n");
            measures.append("      )\n");
            measures.append("      /\n");
            measures.append("      NULLIF(\n");
            measures.append("        SUM(").append(segmentVar).append(".n) * SUM(").append(segmentVar).append(".sum_x2)\n");
            measures.append("        - SUM(").append(segmentVar).append(".sum_x) * SUM(").append(segmentVar).append(".sum_x),\n");
            measures.append("        0\n");
            measures.append("      )\n");
            measures.append("    ) * 180 / pi() AS seg").append(segmentNum).append("_angle,\n");
            
            // Add segment boundaries
            measures.append("    -- Segment ").append(segmentNum).append(" boundaries\n");
            measures.append("    FIRST(").append(segmentVar).append(".bucket_start) AS seg").append(segmentNum).append("_start,\n");
            measures.append("    LAST(").append(segmentVar).append(".bucket_start) + ").append(generateIntervalExpression(timeUnit)).append(" AS seg").append(segmentNum).append("_end");
            
            if (i < segments.size() - 1) {
                measures.append(",\n");
            } else {
                measures.append("\n");
            }
        }
        
        return measures.toString();
    }

    /**
     * Generate the PATTERN clause based on pattern segments and their repetitions.
     */
    private static String generatePattern(List<PatternSegment> segments) {
        List<String> patternParts = new ArrayList<>();
        
        for (PatternSegment segment : segments) {
            String variable = segment.variable;
            RepetitionFactor repetition = segment.repetitionFactor;
            
            // Convert repetition to SQL pattern syntax
            String repetitionStr = convertRepetitionToSQL(repetition);
            patternParts.add(variable + repetitionStr);
        }
        
        return String.join(" ", patternParts);
    }

    /**
     * Convert RepetitionFactor to SQL MATCH_RECOGNIZE pattern syntax.
     */
    private static String convertRepetitionToSQL(RepetitionFactor repetition) {
        int min = repetition.getMinRepetitions();
        int max = repetition.getMaxRepetitions();
        
        if (min == max) {
            return "{" + min + "}";
        } else if (max == Integer.MAX_VALUE) {
            if (min == 0) {
                return "*";
            } else if (min == 1) {
                return "+";
            } else {
                return "{" + min + ",}";
            }
        } else if (min == 0 && max == 1) {
            return "?";
        } else {
            return "{" + min + "," + max + "}";
        }
    }

    /**
     * Generate the DEFINE clause based on segment specifications.
     */
    private static String generateDefine(List<PatternSegment> segments) {
        List<String> defineParts = new ArrayList<>();
        
        for (PatternSegment segment : segments) {
            String variable = segment.variable;
            // For now, define all segments as TRUE - could be enhanced with specific conditions
            // based on TimeFilter constraints if needed
            defineParts.add("    " + variable + " AS TRUE");
        }
        
        return String.join(",\n", defineParts) + "\n";
    }

    /**
     * Generate the WHERE clause for angle constraints based on ValueFilters.
     */
    private static String generateWhereClause(List<PatternSegment> segments) {
        List<String> conditions = new ArrayList<>();
        
        for (int i = 0; i < segments.size(); i++) {
            PatternSegment segment = segments.get(i);
            ValueFilter valueFilter = segment.valueFilter;
            int segmentNum = i + 1;
            
            // Extract angle constraints from ValueFilter
            if (!valueFilter.isValueAny()) {
                double minAngle = valueFilter.getMinDegree();
                double maxAngle = valueFilter.getMaxDegree();
                conditions.add("  seg" + segmentNum + "_angle BETWEEN " + minAngle + " AND " + maxAngle);
            }
        }
        
        return conditions.isEmpty() ? "" : String.join("\n  AND ", conditions) + "\n";
    }

    /**
     * Get the segment variable name (A, B, C, etc.) for the given index.
     */
    private static String getSegmentVariable(int index) {
        return String.valueOf((char) ('A' + index));
    }

    /**
     * Helper class to represent a pattern segment with its constraints.
     */
    private static class PatternSegment {
        final String variable;
        final ValueFilter valueFilter;
        final RepetitionFactor repetitionFactor;
        
        PatternSegment(int index, String variable, ValueFilter valueFilter, 
                      TimeFilter timeFilter, RepetitionFactor repetitionFactor) {
            this.variable = variable;
            this.valueFilter = valueFilter;
            this.repetitionFactor = repetitionFactor;
        }
    }
}
