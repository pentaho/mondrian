package javax.jmi.model;

import javax.jmi.reflect.*;

public interface RefersTo extends RefAssociation {
    public boolean exists(Reference referent, AssociationEnd referencedEnd);
    public java.util.Collection getReferent(AssociationEnd referencedEnd);
    public AssociationEnd getReferencedEnd(Reference referent);
    public boolean add(Reference referent, AssociationEnd referencedEnd);
    public boolean remove(Reference referent, AssociationEnd referencedEnd);
}
