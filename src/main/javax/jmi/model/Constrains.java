package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Constrains extends RefAssociation {
    public boolean exists(Constraint constraint, ModelElement constrainedElement);
    public java.util.Collection getConstraint(ModelElement constrainedElement);
    public java.util.Collection getConstrainedElement(Constraint constraint);
    public boolean add(Constraint constraint, ModelElement constrainedElement);
    public boolean remove(Constraint constraint, ModelElement constrainedElement);
}
