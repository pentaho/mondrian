package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Generalizes extends RefAssociation {
    public boolean exists(GeneralizableElement supertype, GeneralizableElement subtype);
    public java.util.List getSupertype(GeneralizableElement subtype);
    public java.util.Collection getSubtype(GeneralizableElement supertype);
    public boolean add(GeneralizableElement supertype, GeneralizableElement subtype);
    public boolean remove(GeneralizableElement supertype, GeneralizableElement subtype);
}
