package javax.jmi.model;

import javax.jmi.reflect.*;

public interface MofPackageClass extends RefClass {
    public MofPackage createMofPackage();
    public MofPackage createMofPackage(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility);
}
