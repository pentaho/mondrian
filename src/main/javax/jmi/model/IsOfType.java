package javax.jmi.model;

import javax.jmi.reflect.*;

public interface IsOfType extends RefAssociation {
    public boolean exists(Classifier type, TypedElement typedElements);
    public Classifier getType(TypedElement typedElements);
    public java.util.Collection getTypedElements(Classifier type);
    public boolean add(Classifier type, TypedElement typedElements);
    public boolean remove(Classifier type, TypedElement typedElements);
}
