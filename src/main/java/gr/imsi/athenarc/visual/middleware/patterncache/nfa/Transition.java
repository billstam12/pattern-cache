package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

public class Transition {
    private final NFAState target;
    private final TransitionMatcher matcher;

    public Transition(NFAState target, TransitionMatcher matcher) {
        this.target = target;
        this.matcher = matcher;
    }

    public NFAState getTarget() {
        return target;
    }

    public TransitionMatcher getMatcher() {
        return matcher;
    }

    public String toString() {
        return "@Transition " + "target=" + Integer.toHexString(System.identityHashCode(target));
    }
}
