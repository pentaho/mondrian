package javax.jmi.model;

import javax.jmi.reflect.RefClass;

public interface MofExceptionClass extends RefClass {
    public MofException createMofException();
    public MofException createMofException(String name, String annotation, ScopeKind scope, VisibilityKind visibility);
}
