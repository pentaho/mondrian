package javax.jmi.model;

import javax.jmi.reflect.*;

public interface EnumerationTypeClass extends RefClass {
    public EnumerationType createEnumerationType();
    public EnumerationType createEnumerationType(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility, java.util.List labels);
}
