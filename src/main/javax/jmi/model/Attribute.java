package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Attribute extends StructuralFeature {
    public boolean isDerived();
    public void setDerived(boolean newValue);
}
