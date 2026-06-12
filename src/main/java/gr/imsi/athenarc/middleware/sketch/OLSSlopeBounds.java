package gr.imsi.athenarc.middleware.sketch;

/**
 * Tight closed-form OLS-slope bounds when each y_i is interval-valued: y_i ∈ [yMin_i, yMax_i].
 *
 * The OLS slope is linear in the y vector with weights w_i = (x_i − mean(x)) / Σ_j (x_j − mean(x))².
 * The vertex of the y-box that maximises the slope picks yMax_i where w_i > 0 and yMin_i where w_i < 0.
 * The minimising vertex is the symmetric pick. Tight in O(n) — no need to enumerate the 2^n vertices.
 *
 * The bound holds under the bucket-midpoint x assumption (each bucket contributes one representative
 * point at its time-midpoint); it does not bound the slope of the underlying point-level series for which
 * x within a bucket is also uncertain.
 */
public final class OLSSlopeBounds {

    private OLSSlopeBounds() {}

    public static final class Result {
        public final double slopeLower;
        public final double slopeUpper;
        public final boolean defined;

        private Result(double slopeLower, double slopeUpper, boolean defined) {
            this.slopeLower = slopeLower;
            this.slopeUpper = slopeUpper;
            this.defined = defined;
        }

        static Result undefined() {
            return new Result(Double.NaN, Double.NaN, false);
        }

        static Result of(double slopeLower, double slopeUpper) {
            return new Result(slopeLower, slopeUpper, true);
        }
    }

    /**
     * Compute the tight closed-form [slopeLower, slopeUpper] interval for the OLS slope under
     * per-bucket interval uncertainty in y.
     *
     * @param x    bucket-midpoint x coordinates
     * @param yMin per-bucket lower bound on y
     * @param yMax per-bucket upper bound on y (must satisfy yMax[i] ≥ yMin[i])
     * @return result with defined=false if n < 2 or all x equal; otherwise slopeLower ≤ slopeUpper.
     */
    public static Result compute(double[] x, double[] yMin, double[] yMax) {
        if (x == null || yMin == null || yMax == null) {
            throw new IllegalArgumentException("x, yMin, yMax must be non-null");
        }
        int n = x.length;
        if (n != yMin.length || n != yMax.length) {
            throw new IllegalArgumentException("x, yMin, yMax must have equal length");
        }
        if (n < 2) {
            return Result.undefined();
        }

        double meanX = 0.0;
        for (int i = 0; i < n; i++) {
            meanX += x[i];
        }
        meanX /= n;

        double denom = 0.0;
        double numUpper = 0.0;
        double numLower = 0.0;
        for (int i = 0; i < n; i++) {
            double xc = x[i] - meanX;
            denom += xc * xc;
            double yUpperPick;
            double yLowerPick;
            if (xc > 0.0) {
                yUpperPick = yMax[i];
                yLowerPick = yMin[i];
            } else if (xc < 0.0) {
                yUpperPick = yMin[i];
                yLowerPick = yMax[i];
            } else {
                double mid = 0.5 * (yMin[i] + yMax[i]);
                yUpperPick = mid;
                yLowerPick = mid;
            }
            numUpper += xc * yUpperPick;
            numLower += xc * yLowerPick;
        }

        if (denom == 0.0) {
            return Result.undefined();
        }
        return Result.of(numLower / denom, numUpper / denom);
    }
}
