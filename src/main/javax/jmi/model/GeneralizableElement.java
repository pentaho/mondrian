package javax.jmi.model;

import javax.jmi.reflect.*;

public interface GeneralizableElement extends Namespace {
    public java.util.List allSupertypes();
    public ModelElement lookupElementExtended(String name) throws NameNotFoundException;
    public java.util.List findElementsByTypeExtended(MofClass ofType, boolean includeSubtypes);
    public boolean isRoot();
    public void setRoot(boolean newValue);
    public boolean isLeaf();
    public void setLeaf(boolean newValue);
    public boolean isAbstract();
    public void setAbstract(boolean newValue);
    public VisibilityKind getVisibility();
    public void setVisibility(VisibilityKind newValue);
    public java.util.List getSupertypes();
}
