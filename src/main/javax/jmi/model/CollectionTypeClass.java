package javax.jmi.model;

import javax.jmi.reflect.*;

public interface CollectionTypeClass extends RefClass {
    public CollectionType createCollectionType();
    public CollectionType createCollectionType(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility, MultiplicityType multiplicity);
}
