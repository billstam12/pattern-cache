package gr.imsi.athenarc.middleware.domain;

/**
 * Represents a sequence of uni-variate data point that can be traversed in time-ascending order.
 */
public interface DataPoints extends Iterable<DataPoint>, TimeInterval  { }
