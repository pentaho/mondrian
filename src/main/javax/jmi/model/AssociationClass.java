package javax.jmi.model;

import javax.jmi.reflect.*;

public interface AssociationClass extends RefClass {
    public Association createAssociation();
    public Association createAssociation(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility, boolean isDerived);
}
