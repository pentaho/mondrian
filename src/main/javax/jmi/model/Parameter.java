package javax.jmi.model;



public interface Parameter extends TypedElement {
    public DirectionKind getDirection();
    public void setDirection(DirectionKind newValue);
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
}
