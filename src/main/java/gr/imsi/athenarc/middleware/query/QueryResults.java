package gr.imsi.athenarc.middleware.query;

public interface QueryResults {
    
    double getCacheHitRatio();
    long getIoCount();
}
