package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Reference extends StructuralFeature {
    public AssociationEnd getExposedEnd();
    public void setExposedEnd(AssociationEnd newValue);
    public AssociationEnd getReferencedEnd();
    public void setReferencedEnd(AssociationEnd newValue);
}
