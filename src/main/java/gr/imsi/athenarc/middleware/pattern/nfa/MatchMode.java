package gr.imsi.athenarc.middleware.pattern.nfa;

/**
 * Filter-check semantics used by the NFA when validating a candidate segment's
 * combined-sketch slope against the segment's ValueFilter.
 *
 * STRICT: midpoint angle must be in [low, high]. The default. Used by the no-cache
 * pattern path and by MATCH_RECOGNIZE — both have exact slopes (zero uncertainty),
 * so midpoint == bound and the strict check is the natural choice.
 *
 * RELAXED: combined-sketch bound must intersect [low, high]. Used by the cached
 * pattern path on ApproxOLSSketch data. Produces a strict superset of strict
 * matches; the post-NFA classifier then distinguishes confident matches (bound
 * fully inside filter) from ambiguous ones (bound straddles a filter boundary).
 */
public enum MatchMode {
    STRICT,
    RELAXED
}
