package javax.jmi.model;

import javax.jmi.reflect.*;

public interface AliasTypeClass extends RefClass {
    public AliasType createAliasType();
    public AliasType createAliasType(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility);
}
