package javax.jmi.model;

import javax.jmi.reflect.*;

public interface CanRaise extends RefAssociation {
    public boolean exists(Operation operation, MofException except);
    public java.util.Collection getOperation(MofException except);
    public java.util.List getExcept(Operation operation);
    public boolean add(Operation operation, MofException except);
    public boolean remove(Operation operation, MofException except);
}
