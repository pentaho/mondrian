package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Contains extends RefAssociation {
    public boolean exists(Namespace container, ModelElement containedElement);
    public Namespace getContainer(ModelElement containedElement);
    public java.util.List getContainedElement(Namespace container);
    public boolean add(Namespace container, ModelElement containedElement);
    public boolean remove(Namespace container, ModelElement containedElement);
}
