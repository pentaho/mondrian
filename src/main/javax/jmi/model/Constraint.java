package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Constraint extends ModelElement {
    public String getExpression();
    public void setExpression(String newValue);
    public String getLanguage();
    public void setLanguage(String newValue);
    public EvaluationKind getEvaluationPolicy();
    public void setEvaluationPolicy(EvaluationKind newValue);
    public java.util.Collection getConstrainedElements();
}
