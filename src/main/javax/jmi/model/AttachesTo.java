package javax.jmi.model;

import javax.jmi.reflect.*;

public interface AttachesTo extends RefAssociation {
    public boolean exists(ModelElement modelElement, Tag tag);
    public java.util.Collection getModelElement(Tag tag);
    public java.util.List getTag(ModelElement modelElement);
    public boolean add(ModelElement modelElement, Tag tag);
    public boolean remove(ModelElement modelElement, Tag tag);
}
