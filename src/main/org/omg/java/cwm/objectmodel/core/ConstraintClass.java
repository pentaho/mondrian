/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ConstraintClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.Constraint createConstraint( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.BooleanExpression _body )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Constraint createConstraint();

}
