package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Namespace extends ModelElement {
    public ModelElement lookupElement(String name) throws NameNotFoundException;
    public ModelElement resolveQualifiedName(java.util.List qualifiedName) throws NameNotResolvedException;
    public java.util.List findElementsByType(MofClass ofType, boolean includeSubtypes);
    public boolean nameIsValid(String proposedName);
    public java.util.List getContents();
}
