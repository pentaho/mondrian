package javax.jmi.model;

import javax.jmi.reflect.*;

public interface StructuralFeature extends Feature, TypedElement {
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
    public boolean isChangeable();
    public void setChangeable(boolean newValue);
}
