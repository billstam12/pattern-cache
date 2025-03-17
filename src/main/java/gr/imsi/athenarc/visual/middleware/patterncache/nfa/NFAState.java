package gr.imsi.athenarc.visual.middleware.patterncache.nfa;

import java.util.ArrayList;
import java.util.List;

public class NFAState {
    private final List<Transition> transitions = new ArrayList<>();
    private boolean accept;

    public NFAState() {
        this.accept = false;
    }

    public List<Transition> getTransitions() {
        return transitions;
    }

    public boolean isAccept() {
        return accept;
    }

    public void setAccept(boolean accept) {
        this.accept = accept;
    }

    public void addTransition(Transition t) {
        transitions.add(t);
    }

    @Override
    public String toString() {
        return "NFAState@" + Integer.toHexString(System.identityHashCode(this))
            + " (accept=" + isAccept() + ", transitions=" + transitions + ")";
    }
}