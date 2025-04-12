package gr.imsi.athenarc.visual.middleware.query.pattern;

public class ValueFilter {
    // -- Value constraints --
    private final boolean valueAny;   // if true, ignore value constraints
    private final double valueLow;
    private final double valueHigh;

    public ValueFilter(boolean valueAny, double valueLow, double valueHigh) {
        this.valueAny = valueAny;
        this.valueLow = valueLow;
        this.valueHigh = valueHigh;
    }

    public boolean isValueAny() {
        return valueAny;
    }

    public double getValueLow() {
        return valueLow;
    }

    public double getValueHigh() {
        return valueHigh;
    }

    public String toString() {
        return "ValueFilter: " + (valueAny ? "ANY" : "[" + valueLow + ", " + valueHigh + "]");
    }
}
