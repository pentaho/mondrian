package javax.jmi.model;

import javax.jmi.reflect.*;

public interface MultiplicityType extends RefStruct {
    public int getLower();
    public int getUpper();
    public boolean isOrdered();
    public boolean isUnique();
}
