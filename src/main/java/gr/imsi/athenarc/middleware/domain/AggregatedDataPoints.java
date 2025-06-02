package gr.imsi.athenarc.middleware.domain;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface AggregatedDataPoints extends Iterable<AggregatedDataPoint>, TimeInterval { 
    public static final Set<String> SUPPORTED_AGGREGATE_FUNCTIONS = 
        new HashSet<>(Arrays.asList("min", "max", "first", "last", "sum"));
}
