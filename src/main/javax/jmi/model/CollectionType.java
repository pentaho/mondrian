package javax.jmi.model;

import javax.jmi.reflect.*;

public interface CollectionType extends DataType, TypedElement {
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
}
