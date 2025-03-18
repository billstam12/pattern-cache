package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

public class Transition {
    private final NFAState target;
    private final TransitionMatcher matcher;

    private final String label;

    public Transition(NFAState target, TransitionMatcher matcher) {
        this.target = target;
        this.matcher = matcher;
        this.label = "";
    }

    public Transition(NFAState target, TransitionMatcher matcher, String label) {
        this.target = target;
        this.matcher = matcher;
        this.label = label;
    }

    public NFAState getTarget() {
        return target;
    }

    public TransitionMatcher getMatcher() {
        return matcher;
    }

    public String getLabel(){
        return label;
    }

    public String toString() {
        return "@Transition " + "target=" + Integer.toHexString(System.identityHashCode(target)) + "label=" + label ;
    }
}
