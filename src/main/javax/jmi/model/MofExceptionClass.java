package javax.jmi.model;

import javax.jmi.reflect.*;

public interface MofExceptionClass extends RefClass {
    public MofException createMofException();
    public MofException createMofException(String name, String annotation, ScopeKind scope, VisibilityKind visibility);
}
