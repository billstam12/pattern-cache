package gr.imsi.athenarc.middleware.pattern;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.middleware.datasource.DataSource;
import gr.imsi.athenarc.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.middleware.domain.AggregateInterval;
import gr.imsi.athenarc.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.middleware.pattern.PatternMatch;
import gr.imsi.athenarc.middleware.query.pattern.GroupNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternNode;
import gr.imsi.athenarc.middleware.query.pattern.PatternQuery;
import gr.imsi.athenarc.middleware.query.pattern.PatternQueryResults;
import gr.imsi.athenarc.middleware.query.pattern.RepetitionFactor;
import gr.imsi.athenarc.middleware.query.pattern.SegmentSpecification;
import gr.imsi.athenarc.middleware.query.pattern.SingleNode;
import gr.imsi.athenarc.middleware.query.pattern.TimeFilter;
import gr.imsi.athenarc.middleware.query.pattern.ValueFilter;
import gr.imsi.athenarc.middleware.sketch.OLSSketch;
import gr.imsi.athenarc.middleware.sketch.Sketch;

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

            if(dataSource.getQueryExecutor() instanceof SQLQueryExecutor == false) {
                throw new IllegalArgumentException("DataSource's QueryExecutor must be an instance of SQLQueryExecutor");
            }
            
            ResultSet resultSet = ((SQLQueryExecutor)dataSource.getQueryExecutor()).executeDbQuery(sqlQuery);

            // Process result set and create sketches similar to other pattern executors
            List<PatternMatch> matches = processMatchRecognizeResults(resultSet, query, method);
            
            // Create result with the generated SQL query
            PatternQueryResults results = new PatternQueryResults();
            results.setMatches(matches);
            results.setExecutionTime(System.currentTimeMillis() - startTime);
            
            LOG.info("MATCH_RECOGNIZE query executed in {} ms, found {} matches", 
                    results.getExecutionTime(), matches.size());
            
            return results;
            
        } catch (Exception e) {
            LOG.error("Failed to execute MATCH_RECOGNIZE query", e);
            throw new RuntimeException("Failed to execute MATCH_RECOGNIZE query", e);
        }
    }

    /**
     * Process the MATCH_RECOGNIZE query results and convert them directly to pattern matches.
     * Each row in the result set represents one pattern match, with columns like:
     * seg1_start, seg1_end, seg1_angle, seg2_start, seg2_end, seg2_angle, etc.
     * 
     * @param resultSet The result set from the MATCH_RECOGNIZE query
     * @param query The original pattern query
     * @param method The method used (e.g., "ols", "firstLast", etc.)
     * @return List of pattern matches
     */
    private static List<PatternMatch> processMatchRecognizeResults(ResultSet resultSet, PatternQuery query, String method) {
        List<PatternMatch> patternMatches = new ArrayList<>();
        
        try {
            // Determine the number of segments in the pattern
            List<PatternNode> patternNodes = query.getPatternNodes();
            List<PatternSegment> segments = extractPatternSegments(patternNodes);
            int numSegments = segments.size();
            
            LOG.info("Processing MATCH_RECOGNIZE results for {} segments", numSegments);
            
            int matchIndex = 0;
            // Process each row in the result set (each row = one pattern match)
            while (resultSet.next()) {
                List<List<Sketch>> matchSegments = new ArrayList<>();
                
                // For each segment in the pattern, create sketches
                for (int segmentIdx = 0; segmentIdx < numSegments; segmentIdx++) {
                    int segmentNum = segmentIdx + 1; // SQL columns are 1-indexed (seg1, seg2, etc.)
                    
                    try {
                        // Get segment boundaries and angle from result set
                        String startColName = "seg" + segmentNum + "_start";
                        String endColName = "seg" + segmentNum + "_end";
                        String angleColName = "seg" + segmentNum + "_angle";
                        
                        Timestamp startTimestamp = resultSet.getTimestamp(startColName);
                        Timestamp endTimestamp = resultSet.getTimestamp(endColName);
                        double angle = resultSet.getDouble(angleColName);
                        
                        if (startTimestamp != null && endTimestamp != null) {
                            long startTime = startTimestamp.getTime();
                            long endTime = endTimestamp.getTime();
                            
                            // Create a sketch for this segment
                            Sketch sketch = createSketchFromMatchRecognizeResult(startTime, endTime, angle, method);
                            
                            // Each segment contains a list with one sketch
                            List<Sketch> segmentSketches = new ArrayList<>();
                            segmentSketches.add(sketch);
                            matchSegments.add(segmentSketches);
                            
                            LOG.debug("Created sketch for segment {}: start={}, end={}, angle={}", 
                                    segmentNum, startTime, endTime, angle);
                        }
                    } catch (SQLException e) {
                        LOG.warn("Could not process segment {}: {}", segmentNum, e.getMessage());
                    }
                }
                
                if (!matchSegments.isEmpty()) {
                    // Create PatternMatch directly from segments
                    PatternMatch patternMatch = new PatternMatch(matchSegments);
                    patternMatches.add(patternMatch);
                }
            }
            
            LOG.info("Total pattern matches found: {}", patternMatches.size());
            
        } catch (SQLException e) {
            LOG.error("Error processing MATCH_RECOGNIZE results", e);
            throw new RuntimeException("Error processing MATCH_RECOGNIZE results", e);
        }
        
        return patternMatches;
    }

    /**
     * Create a sketch from MATCH_RECOGNIZE result data.
     * 
     * @param startTime Start time of the segment in milliseconds
     * @param endTime End time of the segment in milliseconds
     * @param angle The calculated angle/slope from the SQL query
     * @param method The method type to determine which sketch type to create
     * @return A sketch representing this segment
     */
    private static Sketch createSketchFromMatchRecognizeResult(long startTime, long endTime, double angle, String method) {
        // For now, create an OLS sketch since MATCH_RECOGNIZE calculates regression angles
        // This could be extended to support other sketch types based on the method parameter
        OLSSketch sketch = new OLSSketch(startTime, endTime, 0);
        
        // Since we already have the calculated angle from SQL, we need to set it directly
        // We'll use reflection or create a specialized constructor/setter for this case
        // For now, we mark it as initialized and set the angle (this might need adjustment based on OLSSketch implementation)
        try {
            java.lang.reflect.Field angleField = OLSSketch.class.getDeclaredField("angle");
            angleField.setAccessible(true);
            angleField.set(sketch, angle);
            
            java.lang.reflect.Field hasInitializedField = OLSSketch.class.getDeclaredField("hasInitialized");
            hasInitializedField.setAccessible(true);
            hasInitializedField.set(sketch, true);
            
            LOG.debug("Set angle {} for sketch covering period {} to {}", angle, startTime, endTime);
        } catch (Exception e) {
            LOG.warn("Could not set angle directly on sketch, using default: {}", e.getMessage());
        }
        
        return sketch;
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
        String measure = dataSource.getDataset().getHeader()[measureId];
        AggregateInterval timeUnit = query.getTimeUnit();
        List<PatternNode> patternNodes = query.getPatternNodes();

        long alignedFrom = DateTimeUtil.alignToTimeUnitBoundary(query.getFrom(), query.getTimeUnit(), true);
        long alignedTo = DateTimeUtil.alignToTimeUnitBoundary(query.getTo(), query.getTimeUnit(), false);

        // Generate the bucketing interval
        String bucketingExpression = generateBucketingExpression(timeUnit);
        
        // Extract pattern segments and their constraints
        List<PatternSegment> segments = extractPatternSegments(patternNodes);
        
        // Build the main query with normalized timestamps (0-1 across entire query range)
        sql.append("SELECT *\n");
        sql.append("FROM (\n");
        sql.append("  SELECT\n");
        sql.append("    bucket_start,\n");
        sql.append("    SUM(normalized_time) AS sum_x,\n");
        sql.append("    SUM(value) AS sum_y,\n");
        sql.append("    SUM(normalized_time * value) AS sum_xy,\n");
        sql.append("    SUM(normalized_time * normalized_time) AS sum_x2,\n");
        sql.append("    COUNT(*) AS n\n");
        sql.append("  FROM (\n");
        sql.append("    SELECT\n");
        sql.append("      ").append(bucketingExpression).append(" AS bucket_start,\n");
        sql.append("      value,\n");
        sql.append("      -- Normalize timestamp to 0-1 range inside the bucket, and then offset by the buckets place inside the time series\n");

        long bucketIntervalMs = timeUnit.toDuration().toMillis();
        long alignedStart = DateTimeUtil.alignToTimeUnitBoundary(dataSource.getDataset().getTimeRange().getFrom(), timeUnit, true);
        sql.append(     "      FLOOR((to_unixtime(timestamp) * 1000 - ").append(alignedStart).append(") / ").append(bucketIntervalMs).append(") + \n");
        sql.append("      ((to_unixtime(timestamp) * 1000 - ").append(alignedStart).append(") % ").append(bucketIntervalMs).append(") / ").append(bucketIntervalMs).append(".0 AS normalized_time\n");
       
        sql.append("    FROM ").append(tableName).append("\n");
        sql.append("    WHERE id = '").append(measure).append("'\n");
        
        // Add time range filter if specified
        if (alignedFrom > 0 && alignedTo > 0) {
            sql.append("      AND timestamp >= TIMESTAMP '").append(DateTimeUtil.format(alignedFrom)).append("'\n");
            sql.append("      AND timestamp < TIMESTAMP '").append(DateTimeUtil.format(alignedTo)).append("'\n");
        }
        
        sql.append("  ) normalized_data\n");
        sql.append("  GROUP BY bucket_start\n");
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
        
        sql.append("ORDER BY seg1_start");
        
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
                return String.format("FROM_UNIXTIME(FLOOR(to_unixtime(timestamp) * 1000 / %d) * %d / 1000)", 
                    multiplier, multiplier);
                    
            case SECONDS:
                // For seconds, use epoch seconds and round down
                return String.format("FROM_UNIXTIME(FLOOR(to_unixtime(timestamp) / %d) * %d)", 
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
                // Trino doesn't support MILLISECOND intervals directly
                // Add milliseconds using timestamp arithmetic
                if (multiplier == 1) {
                    return "INTERVAL '0.001' SECOND";
                } else {
                    return String.format("INTERVAL '%f' SECOND", multiplier / 1000.0);
                }
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
     * Extract pattern segments from PatternNodes, preserving group structure for repetitions.
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
            
            // For SingleNode, create one segment with time filter as segment length
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
            
            // For GroupNode, extract all children with their own repetitions
            List<PatternNode> children = groupNode.getChildren();
            int childIndex = startIndex;
            int groupId = startIndex; // Use start index as group identifier
            
            for (PatternNode child : children) {
                if (child instanceof SingleNode) {
                    SingleNode singleChild = (SingleNode) child;
                    SegmentSpecification spec = singleChild.getSpec();
                    RepetitionFactor childRepetition = singleChild.getRepetitionFactor();
                    
                    // Create segment with child's own repetition and group information
                    PatternSegment segment = new PatternSegment(
                        childIndex,
                        getSegmentVariable(childIndex),
                        spec.getValueFilter(),
                        spec.getTimeFilter(),
                        childRepetition, // Use child's own repetition
                        groupId, // Track which group this segment belongs to
                        groupRepetition // Store group repetition for pattern generation
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
        
        int i = 0;
        while (i < segments.size()) {
            PatternSegment segment = segments.get(i);
            
            if (segment.groupId != null) {
                // This segment is part of a group - collect all segments with the same group ID
                List<PatternSegment> groupSegments = new ArrayList<>();
                Integer currentGroupId = segment.groupId;
                RepetitionFactor groupRepetition = segment.groupRepetitionFactor;
                
                // Collect all segments belonging to the same group
                while (i < segments.size() && segments.get(i).groupId != null && 
                       segments.get(i).groupId.equals(currentGroupId)) {
                    groupSegments.add(segments.get(i));
                    i++;
                }
                
                // Generate pattern for the entire group
                List<String> groupPatternParts = new ArrayList<>();
                for (PatternSegment groupSegment : groupSegments) {
                    String variable = groupSegment.variable;
                    TimeFilter timeFilter = groupSegment.timeFilter;
                    RepetitionFactor individualRepetition = groupSegment.repetitionFactor;
                    
                    // Apply individual segment's time filter and repetition
                    String segmentPattern = generateSegmentPattern(variable, timeFilter, individualRepetition);
                    groupPatternParts.add(segmentPattern);
                }
                
                // Combine group segments and apply group repetition
                String groupPattern = "(" + String.join(" ", groupPatternParts) + ")";
                String groupKleeneOperator = convertRepetitionToSQL(groupRepetition);
                if (!groupKleeneOperator.isEmpty() && !groupKleeneOperator.equals("{1}")) {
                    groupPattern = groupPattern + groupKleeneOperator;
                }
                
                patternParts.add(groupPattern);
                
            } else {
                // This segment is not part of a group - handle individually
                String variable = segment.variable;
                TimeFilter timeFilter = segment.timeFilter;
                RepetitionFactor repetition = segment.repetitionFactor;
                
                String segmentPattern = generateSegmentPattern(variable, timeFilter, repetition);
                patternParts.add(segmentPattern);
                i++;
            }
        }
        
        return String.join(" ", patternParts);
    }
    
    /**
     * Generate pattern string for a single segment with its time filter and repetition.
     */
    private static String generateSegmentPattern(String variable, TimeFilter timeFilter, RepetitionFactor repetition) {
        // First, apply the time filter as segment length ({n})
        String segmentWithLength = variable;
        if (timeFilter != null && !timeFilter.isTimeAny()) {
            // For MATCH_RECOGNIZE, we need to determine the segment length
            // If timeLow == timeHigh, use exact length {n}
            // If they differ, we might use a range {min,max} but MATCH_RECOGNIZE syntax is limited
            int timeLow = timeFilter.getTimeLow();
            int timeHigh = timeFilter.getTimeHigh();
            
            if (timeLow == timeHigh) {
                // Exact length
                segmentWithLength = variable + "{" + timeLow + "}";
            } else if (timeHigh == Integer.MAX_VALUE) {
                // Minimum length with no upper bound
                segmentWithLength = variable + "{" + timeLow + ",}";
            } else {
                // Range with both min and max
                segmentWithLength = variable + "{" + timeLow + "," + timeHigh + "}";
            }
        }
        
        // Then, apply kleene repetition operators (*, +, ?) if any
        String kleeneOperator = convertRepetitionToSQL(repetition);
        if (!kleeneOperator.isEmpty() && !kleeneOperator.equals("{1}")) {
            // Apply kleene operator to the entire segment (including its length)
            segmentWithLength = "(" + segmentWithLength + ")" + kleeneOperator;
        }
        
        return segmentWithLength;
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
        final TimeFilter timeFilter;        // TimeFilter determines segment length ({n})
        final RepetitionFactor repetitionFactor; // RepetitionFactor for kleene operators (*, +, ?)
        final Integer groupId; // Group ID if this segment is part of a group (null otherwise)
        final RepetitionFactor groupRepetitionFactor; // Group repetition factor (null if not part of a group)
        
        PatternSegment(int index, String variable, ValueFilter valueFilter, 
                      TimeFilter timeFilter, RepetitionFactor repetitionFactor) {
            this.variable = variable;
            this.valueFilter = valueFilter;
            this.timeFilter = timeFilter;
            this.repetitionFactor = repetitionFactor;
            this.groupId = null;
            this.groupRepetitionFactor = null;
        }
        
        PatternSegment(int index, String variable, ValueFilter valueFilter, 
                      TimeFilter timeFilter, RepetitionFactor repetitionFactor,
                      Integer groupId, RepetitionFactor groupRepetitionFactor) {
            this.variable = variable;
            this.valueFilter = valueFilter;
            this.timeFilter = timeFilter;
            this.repetitionFactor = repetitionFactor;
            this.groupId = groupId;
            this.groupRepetitionFactor = groupRepetitionFactor;
        }
    }
}
