package javax.jmi.model;

import javax.jmi.reflect.*;

public class NameNotResolvedException extends RefException {
    private final String explanation;
    private final java.util.List restOfName;
    public NameNotResolvedException(String explanation, java.util.List restOfName) {
        super("explanation: " + explanation + ", restOfName: " + restOfName);
        this.explanation = explanation;
        this.restOfName = restOfName;
    }
    public String getExplanation() {
        return explanation;
    }
    public java.util.List getRestOfName() {
        return restOfName;
    }
}
