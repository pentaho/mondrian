package javax.jmi.model;

import javax.jmi.reflect.*;

public interface AssociationEnd extends TypedElement {
    public AssociationEnd otherEnd();
    public boolean isNavigable();
    public void setNavigable(boolean newValue);
    public AggregationKind getAggregation();
    public void setAggregation(AggregationKind newValue);
    public MultiplicityType getMultiplicity();
    public void setMultiplicity(MultiplicityType newValue);
    public boolean isChangeable();
    public void setChangeable(boolean newValue);
}
