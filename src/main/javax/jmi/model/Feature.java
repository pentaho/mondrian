package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Feature extends ModelElement {
    public ScopeKind getScope();
    public void setScope(ScopeKind newValue);
    public VisibilityKind getVisibility();
    public void setVisibility(VisibilityKind newValue);
}
