package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ReferenceClass extends RefClass {
    public Reference createReference();
    public Reference createReference(String name, String annotation, ScopeKind scope, VisibilityKind visibility, MultiplicityType multiplicity, boolean isChangeable);
}
