package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Import extends ModelElement {
    public VisibilityKind getVisibility();
    public void setVisibility(VisibilityKind newValue);
    public boolean isClustered();
    public void setClustered(boolean newValue);
    public Namespace getImportedNamespace();
    public void setImportedNamespace(Namespace newValue);
}
