package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Association extends Classifier {
    public boolean isDerived();
    public void setDerived(boolean newValue);
}
