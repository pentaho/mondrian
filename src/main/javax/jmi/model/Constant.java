package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Constant extends TypedElement {
    public String getValue();
    public void setValue(String newValue);
}
