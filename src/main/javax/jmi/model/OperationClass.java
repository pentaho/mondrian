package javax.jmi.model;

import javax.jmi.reflect.*;

public interface OperationClass extends RefClass {
    public Operation createOperation();
    public Operation createOperation(String name, String annotation, ScopeKind scope, VisibilityKind visibility, boolean isQuery);
}
