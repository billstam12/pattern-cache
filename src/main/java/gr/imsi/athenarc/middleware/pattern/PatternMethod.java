package gr.imsi.athenarc.middleware.pattern;

/**
 * Aggregation flavor used by the pattern executor.
 *
 * <ul>
 *   <li>{@link #OLS} — full per-bucket OLS sums (zero bound width). Used by the
 *       no-cache pattern path, MATCH_RECOGNIZE, and the OLS-verify step inside
 *       the cached path.</li>
 *   <li>{@link #APPROX_OLS} — sub-bucket MinMax + slope bound (non-zero bound
 *       width, refinable). Used by the cached pattern path.</li>
 * </ul>
 *
 * <p>The {@code canonical} form ({@code "ols"} / {@code "approxOls"}) is the
 * legacy string used by the cache/datasource SQL layer and by the Experiments
 * harness CLI; {@link #from(String)} converts at the boundary.
 */
public enum PatternMethod {
    OLS("ols"),
    APPROX_OLS("approxOls");

    private final String canonical;

    PatternMethod(String canonical) {
        this.canonical = canonical;
    }

    public String canonical() {
        return canonical;
    }

    public static PatternMethod from(String s) {
        if (s == null) {
            throw new IllegalArgumentException("PatternMethod string must not be null");
        }
        for (PatternMethod m : values()) {
            if (m.canonical.equalsIgnoreCase(s)) return m;
        }
        throw new IllegalArgumentException("Unknown pattern method: " + s);
    }

    @Override
    public String toString() {
        return canonical;
    }
}
