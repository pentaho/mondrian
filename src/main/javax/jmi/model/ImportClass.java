package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ImportClass extends RefClass {
    public Import createImport();
    public Import createImport(String name, String annotation, VisibilityKind visibility, boolean isClustered);
}
