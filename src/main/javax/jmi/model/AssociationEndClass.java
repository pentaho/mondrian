package javax.jmi.model;

import javax.jmi.reflect.RefClass;

public interface AssociationEndClass extends RefClass {
    public AssociationEnd createAssociationEnd();
    public AssociationEnd createAssociationEnd(String name, String annotation, boolean isNavigable, AggregationKind aggregation, MultiplicityType multiplicity, boolean isChangeable);
}
