package javax.jmi.model;

import javax.jmi.reflect.*;

public interface PrimitiveTypeClass extends RefClass {
    public PrimitiveType createPrimitiveType();
    public PrimitiveType createPrimitiveType(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility);
}
