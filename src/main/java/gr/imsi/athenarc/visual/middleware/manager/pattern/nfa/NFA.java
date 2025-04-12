package gr.imsi.athenarc.visual.middleware.manager.pattern.nfa;

import java.util.ArrayList;
import java.util.List;

public class NFA {
    private NFAState startState;
    private final List<NFAState> states = new ArrayList<>();

    public NFA() {
        // By default, create a start state that is not accepting
        this.startState = new NFAState();
        states.add(startState);
    }

    public NFAState getStartState() {
        return startState;
    }

    public void setStartState(NFAState state) {
        this.startState = state;
        if (!states.contains(state)) {
            states.add(state);
        }
    }

    public NFAState createState(boolean isAccept) {
        NFAState s = new NFAState();
        s.setAccept(isAccept);
        states.add(s);
        return s;
    }

    public List<NFAState> getStates() {
        return states;
    }
}
