/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface MethodClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.behavioral.Method createMethod( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.ScopeKind _ownerScope, boolean _isQuery, org.omg.java.cwm.objectmodel.core.ProcedureExpression _body )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.Method createMethod();

}
