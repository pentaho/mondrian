package javax.jmi.model;

import javax.jmi.reflect.*;

public interface MofClassClass extends RefClass {
    public MofClass createMofClass();
    public MofClass createMofClass(String name, String annotation, boolean isRoot, boolean isLeaf, boolean isAbstract, VisibilityKind visibility, boolean isSingleton);
}
