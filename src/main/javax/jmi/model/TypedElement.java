package javax.jmi.model;

import javax.jmi.reflect.*;

public interface TypedElement extends ModelElement {
    public Classifier getType();
    public void setType(Classifier newValue);
}
