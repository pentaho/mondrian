package javax.jmi.model;

import javax.jmi.reflect.*;

public interface DependsOn extends RefAssociation {
    public boolean exists(ModelElement dependent, ModelElement provider);
    public java.util.Collection getDependent(ModelElement provider);
    public java.util.Collection getProvider(ModelElement dependent);
}
