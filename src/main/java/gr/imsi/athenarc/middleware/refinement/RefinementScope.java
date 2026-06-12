package gr.imsi.athenarc.middleware.refinement;

/**
 * Selects which visual executor the {@code VisualQueryManager} instantiates.
 * The two scopes are separate executors with structurally different fetch
 * ladders, not a single executor with branched logic.
 */
public enum RefinementScope {
    /**
     * Single-fetch ladder: cache-only error → refetch at refined α over the full
     * range (or capped → M4 over the full range). Implemented by {@code
     * FullVisualQueryExecutor}.
     */
    FULL,

    /**
     * Three-step ladder: patch data-missing at the current α → refine
     * high-error at the refined α → scoped M4 fallback over residual
     * high-error columns. Implemented by {@code ScopedVisualQueryExecutor}.
     */
    SCOPED;

    public static RefinementScope from(String s) {
        if (s == null) return FULL;
        switch (s.trim().toLowerCase()) {
            case "full":   return FULL;
            case "scoped": return SCOPED;
            default:
                throw new IllegalArgumentException("Unknown RefinementScope: " + s);
        }
    }
}
