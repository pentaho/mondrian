/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface OperationClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.behavioral.Operation createOperation( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.ScopeKind _ownerScope, boolean _isQuery, boolean _isAbstract )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.Operation createOperation();

}
