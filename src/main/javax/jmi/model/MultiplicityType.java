package javax.jmi.model;

import javax.jmi.reflect.RefStruct;

public interface MultiplicityType extends RefStruct {
    public int getLower();
    public int getUpper();
    public boolean isOrdered();
    public boolean isUnique();
}
