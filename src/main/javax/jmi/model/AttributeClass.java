package javax.jmi.model;

import javax.jmi.reflect.*;

public interface AttributeClass extends RefClass {
    public Attribute createAttribute();
    public Attribute createAttribute(String name, String annotation, ScopeKind scope, VisibilityKind visibility, MultiplicityType multiplicity, boolean isChangeable, boolean isDerived);
}
