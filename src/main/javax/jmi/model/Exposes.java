package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Exposes extends RefAssociation {
    public boolean exists(Reference referrer, AssociationEnd exposedEnd);
    public java.util.Collection getReferrer(AssociationEnd exposedEnd);
    public AssociationEnd getExposedEnd(Reference referrer);
    public boolean add(Reference referrer, AssociationEnd exposedEnd);
    public boolean remove(Reference referrer, AssociationEnd exposedEnd);
}
