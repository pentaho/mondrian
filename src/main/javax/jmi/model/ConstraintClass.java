package javax.jmi.model;

import javax.jmi.reflect.RefClass;

public interface ConstraintClass extends RefClass {
    public Constraint createConstraint();
    public Constraint createConstraint(String name, String annotation, String expression, String language, EvaluationKind evaluationPolicy);
}
