package javax.jmi.model;



public interface StructuralFeature extends Feature, TypedElement {
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
    public boolean isChangeable();
    public void setChangeable(boolean newValue);
}
