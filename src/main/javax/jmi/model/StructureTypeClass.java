package javax.jmi.model;

import javax.jmi.reflect.*;

public interface StructureTypeClass extends RefClass {
    public StructureType createStructureType();
    public StructureType createStructureType(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility);
}
