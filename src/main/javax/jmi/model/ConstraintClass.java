package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ConstraintClass extends RefClass {
    public Constraint createConstraint();
    public Constraint createConstraint(String name, String annotation, String expression, String language, EvaluationKind evaluationPolicy);
}
