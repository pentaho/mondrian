package javax.jmi.model;



public interface CollectionType extends DataType, TypedElement {
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
}
