package javax.jmi.model;



public interface Feature extends ModelElement {
    public ScopeKind getScope();
    public void setScope(ScopeKind newValue);
    public VisibilityKind getVisibility();
    public void setVisibility(VisibilityKind newValue);
}
