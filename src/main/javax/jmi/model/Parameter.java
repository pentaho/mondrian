package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Parameter extends TypedElement {
    public DirectionKind getDirection();
    public void setDirection(DirectionKind newValue);
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
}
