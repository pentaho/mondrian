package javax.jmi.model;

import javax.jmi.reflect.*;

public interface MofClass extends Classifier {
    public boolean isSingleton();
    public void setSingleton(boolean newValue);
}
